package com.spbsu.flamestream.runtime.tick;

import akka.actor.ActorIdentity;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Identify;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.range.HashRange;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class TickRoutesResolver extends LoggingActor {
  private final Map<Integer, ActorPath> cluster;
  private final TickInfo tickInfo;
  private final Map<HashRange, ActorSelection> tmpRanges = new HashMap<>();
  private final Map<HashRange, ActorRef> refs = new HashMap<>();
  private ActorSelection tmpAcker;
  private ActorRef acker;

  private TickRoutesResolver(Map<Integer, ActorPath> cluster, TickInfo tickInfo) {
    this.cluster = new HashMap<>(cluster);
    this.tickInfo = tickInfo;
  }

  public static Props props(Map<Integer, ActorPath> cluster, TickInfo tickInfo) {
    return Props.create(TickRoutesResolver.class, cluster, tickInfo);
  }

  @Override
  public void preStart() throws Exception {
    log().info("Identifying {}", cluster);
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
    return ReceiveBuilder.create().match(ActorIdentity.class, id -> id.getActorRef().isPresent(), id -> {
      log().info("Got identity {}", id);
      if (id.correlationId() instanceof HashRange) {
        refs.put((HashRange) id.correlationId(), id.getActorRef().get());
      } else if (id.correlationId() instanceof String) {
        acker = id.getActorRef().get();
      } else {
        unhandled(id);
      }

      if (refs.size() == cluster.size() && acker != null) {
        log().info("Collected all refs!");
        getContext().getParent().tell(new TickRoutes(refs, acker), self());
        context().stop(self());
      }
    }).match(ActorIdentity.class, id -> !id.getActorRef().isPresent(), id -> {
      log().info("Got empty identity {}", id);
      if (id.correlationId() instanceof HashRange) {
        context().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                () -> tmpRanges.get(id.correlationId()).tell(new Identify(id.correlationId()), self()),
                context().dispatcher()
        );
      } else if (id.correlationId() instanceof String) {
        context().system().scheduler().scheduleOnce(
                Duration.create(10, TimeUnit.MILLISECONDS),
                () -> tmpAcker.tell(new Identify("Hey tmpAcker"), self()),
                context().dispatcher()
        );
      } else {
        unhandled(id);
      }
    }).build();
  }
}
