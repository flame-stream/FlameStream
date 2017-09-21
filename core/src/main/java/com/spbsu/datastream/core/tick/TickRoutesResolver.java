package com.spbsu.datastream.core.tick;

import akka.actor.ActorIdentity;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Identify;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.configuration.HashRange;

import java.util.HashMap;
import java.util.Map;

public final class TickRoutesResolver extends LoggingActor {
  private final Map<Integer, ActorPath> cluster;
  private final TickInfo tickInfo;

  private ActorSelection tmpAcker;
  private final Map<HashRange, ActorSelection> tmpRanges = new HashMap<>();

  private ActorRef acker;
  private final Map<HashRange, ActorRef> refs = new HashMap<>();

  private TickRoutesResolver(Map<Integer, ActorPath> cluster, TickInfo tickInfo) {
    this.cluster = new HashMap<>(cluster);
    this.tickInfo = tickInfo;
  }

  public static Props props(Map<Integer, ActorPath> cluster, TickInfo tickInfo) {
    return Props.create(TickRoutesResolver.class, cluster, tickInfo);
  }

  @Override
  public void preStart() throws Exception {
    LOG().info("Identifying {}", cluster);
    final Map<HashRange, Integer> map = tickInfo.hashMapping();
    map.forEach((range, id) -> {
      final ActorPath path = cluster.get(id).child(range.toString());
      final ActorSelection selection = context().actorSelection(path);
      tmpRanges.put(range, selection);
      selection.tell(new Identify(range), self());
    });

    tmpAcker = context().actorSelection(cluster.get(tickInfo.ackerLocation()).child("acker"));
    tmpAcker.tell(new Identify("Hey tmpAcker"), self());

    super.preStart();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(ActorIdentity.class, id -> id.getActorRef().isPresent(), id -> {
              LOG().info("Got identity {}", id);
              if (id.correlationId() instanceof HashRange) {
                refs.put((HashRange) id.correlationId(), id.getActorRef().get());
              } else if (id.correlationId() instanceof String) {
                acker = id.getActorRef().get();
              } else {
                unhandled(id);
              }

              if (refs.size() == cluster.size() && acker != null) {
                LOG().info("Collected all refs!");
                getContext().getParent().tell(new TickRoutes(refs, acker), self());
                context().stop(self());
              }
            })
            .match(ActorIdentity.class, id -> !id.getActorRef().isPresent(), id -> {
              LOG().info("Got empty identity {}", id);
              if (id.correlationId() instanceof HashRange) {
                tmpRanges.get(id.correlationId()).tell(new Identify(id.correlationId()), self());
              } else if (id.correlationId() instanceof String) {
                tmpAcker.tell(new Identify("Hey tmpAcker"), self());
              } else {
                unhandled(id);
              }
            })
            .build();
  }
}
