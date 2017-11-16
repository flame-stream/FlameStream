package com.spbsu.flamestream.runtime.node.tick;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.acker.AckActor;
import com.spbsu.flamestream.runtime.node.tick.api.StartTick;
import com.spbsu.flamestream.runtime.node.tick.api.TickCommitDone;
import com.spbsu.flamestream.runtime.node.tick.api.TickInfo;
import com.spbsu.flamestream.runtime.node.tick.api.TickRoutes;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.node.tick.range.HashRange;
import com.spbsu.flamestream.runtime.node.tick.range.RangeConcierge;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class TickConcierge extends LoggingActor {
  private final TickInfo tickInfo;
  private final String localId;

  private final Set<Long> awaitedTicks;

  @Nullable
  private TickRoutes routes = null;

  private TickConcierge(TickInfo tickInfo, String localId, Map<String, ActorPath> cluster, ActorRef tickWatcher) {
    this.tickInfo = tickInfo;
    this.localId = localId;
    this.awaitedTicks = new HashSet<>(tickInfo.tickDependencies());

    myRanges(tickInfo.hashMapping()).forEach(this::rangeConcierge);
    if (tickInfo.ackerLocation().equals(localId)) {
      context().actorOf(AckActor.props(tickInfo, tickWatcher), "acker");
    }

    context().actorOf(TickRoutesResolver.props(cluster, tickInfo), "resolver");
  }

  public static Props props(TickInfo tickInfo, String localId, Map<String, ActorPath> cluster, ActorRef tickWatcher) {
    return Props.create(TickConcierge.class, tickInfo, localId, cluster, tickWatcher);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create().match(TickCommitDone.class, committed -> {
      awaitedTicks.remove(committed.tickId());
      if (awaitedTicks.isEmpty() && routes != null) {
        run();
      }
    }).match(TickRoutes.class, routes -> {
      this.routes = routes;
      if (awaitedTicks.isEmpty() && routes != null) {
        run();
      }
    }).matchAny(m -> stash()).build();
  }

  private void run() {
    unstashAll();
    getContext().getChildren().forEach(c -> c.tell(new StartTick(routes), self()));
    getContext().become(tickRunning());
  }

  private Receive tickRunning() {
    return ReceiveBuilder.create()
            .match(TickCommitDone.class, committed -> {
              if (committed.tickId() == tickInfo.id()) {
                log().info("My job is done here");
                context().stop(self());
              } else {
                unhandled(committed);
              }
            })
            .build();
  }

  private ActorRef rangeConcierge(HashRange range) {
    return context().actorOf(RangeConcierge.props(tickInfo, range), range.toString());
  }

  private Iterable<HashRange> myRanges(Map<HashRange, String> mappings) {
    return mappings.entrySet()
            .stream()
            .filter(e -> e.getValue().equals(localId))
            .map(Map.Entry::getKey)
            .collect(toSet());
  }
}
