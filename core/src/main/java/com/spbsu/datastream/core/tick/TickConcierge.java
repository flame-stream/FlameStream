package com.spbsu.datastream.core.tick;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.AckActor;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.range.RangeConcierge;

import java.util.Map;

import static java.util.stream.Collectors.*;

public final class TickConcierge extends LoggingActor {
  private final TickInfo info;
  private final int localId;

  private TickConcierge(TickInfo tickInfo, int localId, Map<Integer, ActorPath> cluster) {
    this.info = tickInfo;
    this.localId = localId;

    myRanges(tickInfo.hashMapping()).forEach(this::rangeConcierge);
    if (tickInfo.ackerLocation() == localId) {
      context().actorOf(AckActor.props(tickInfo), "acker");
    }

    context().actorOf(TickRoutesResolver.props(cluster, tickInfo), "resolver");
  }

  public static Props props(TickInfo tickInfo, int localId, Map<Integer, ActorPath> cluster) {
    return Props.create(TickConcierge.class, tickInfo, localId, cluster);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(TickRoutes.class, routes -> {
              getContext().getChildren().forEach(c -> c.tell(new StartTick(routes), self()));
              getContext().become(emptyBehavior());
            })
            .build();
  }

  private ActorRef rangeConcierge(HashRange range) {
    return context().actorOf(RangeConcierge.props(info, range), range.toString());
  }

  private Iterable<HashRange> myRanges(Map<HashRange, Integer> mappings) {
    return mappings.entrySet().stream()
            .filter(e -> e.getValue().equals(localId))
            .map(Map.Entry::getKey).collect(toSet());
  }
}
