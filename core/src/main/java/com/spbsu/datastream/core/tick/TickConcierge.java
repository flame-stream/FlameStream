package com.spbsu.datastream.core.tick;

import akka.actor.ActorIdentity;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Identify;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.AckActor;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.range.RangeConcierge;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public final class TickConcierge extends LoggingActor {
  private final TickInfo info;
  private final int localId;

  private final Map<Integer, ActorPath> cluster;

  private TickConcierge(TickInfo tickInfo, int localId, Map<Integer, ActorPath> cluster) {
    this.info = tickInfo;
    this.localId = localId;
    this.cluster = cluster;

    myRanges(tickInfo.hashMapping().asMap())
            .forEach(this::rangeConcierge);
    if (tickInfo.ackerLocation() == localId) {
      context().actorOf(AckActor.props(tickInfo), "acker");
    }
  }

  private final Map<HashRange, ActorSelection> tmpRanges = new HashMap<>();

  public static Props props(TickInfo tickInfo, int localId, Map<Integer, ActorPath> cluster) {
    return Props.create(TickConcierge.class, tickInfo, localId, cluster);
  }

  private ActorSelection acker;

  @Override
  public void preStart() throws Exception {
    final Map<HashRange, Integer> map = info.hashMapping().asMap();
    map.forEach((range, id) -> {
      final ActorPath path = cluster.get(id).child(range.toString());
      final ActorSelection selection = context().actorSelection(path);
      tmpRanges.put(range, selection);
      selection.tell(new Identify(range), self());
    });

    acker = context().actorSelection(cluster.get(info.ackerLocation()).child("acker"));
    acker.tell(new Identify("Hey acker"), self());

    super.preStart();
  }

  private final Map<HashRange, ActorRef> refs = new HashMap<>();

  @Nullable
  private ActorRef ackerRef;

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(ActorIdentity.class, id -> id.getActorRef().isPresent(), id -> {
              if (id.correlationId() instanceof HashRange) {
                refs.put((HashRange) id.correlationId(), sender());
              } else if (id.correlationId() instanceof String) {
                ackerRef = sender();
              } else {
                unhandled(id);
              }

              if (refs.size() == cluster.size() && acker != null) {
                LOG().info("Collected all refs!");
                getContext().getChildren()
                        .forEach(c -> c.tell(new StartTick(new RoutingInfo(refs, ackerRef)), self()));
              }
              getContext().become(emptyBehavior());
            })
            .match(ActorIdentity.class, id -> !id.getActorRef().isPresent(), id -> {
              if (id.correlationId() instanceof HashRange) {
                tmpRanges.get(id.correlationId()).tell(new Identify(id.correlationId()), self());
              } else if (id.correlationId() instanceof String) {
                acker.tell(new Identify("Hey acker"), self());
              } else {
                unhandled(id);
              }
            })
            .build();
  }

  private ActorRef rangeConcierge(HashRange range) {
    return context().actorOf(RangeConcierge.props(info), range.toString());
  }

  private Iterable<HashRange> myRanges(Map<HashRange, Integer> mappings) {
    return mappings.entrySet().stream().filter(e -> e.getValue().equals(localId))
            .map(Map.Entry::getKey).collect(Collectors.toSet());
  }
}
