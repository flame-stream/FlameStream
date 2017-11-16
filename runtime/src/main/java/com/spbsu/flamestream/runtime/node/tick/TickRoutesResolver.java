package com.spbsu.flamestream.runtime.node.tick;

import akka.actor.ActorIdentity;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Identify;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.node.tick.api.TickInfo;
import com.spbsu.flamestream.runtime.node.tick.api.TickRoutes;
import com.spbsu.flamestream.runtime.node.tick.range.HashRange;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class TickRoutesResolver extends LoggingActor {
  private final Map<String, ActorPath> cluster;
  private final TickInfo tickInfo;
  private final Map<HashRange, ActorSelection> tmpRanges = new HashMap<>();
  private final Map<HashRange, ActorRef> refs = new HashMap<>();
  private ActorSelection tmpAcker = null;
  private ActorRef acker = null;

  private TickRoutesResolver(Map<String, ActorPath> cluster, TickInfo tickInfo) {
    this.cluster = new HashMap<>(cluster);
    this.tickInfo = tickInfo;
  }

  static Props props(Map<String, ActorPath> cluster, TickInfo tickInfo) {
    return Props.create(TickRoutesResolver.class, cluster, tickInfo);
  }

  @Override
  public void preStart() throws Exception {
    log().info("Identifying {}", cluster);
    final Map<HashRange, String> map = tickInfo.hashMapping();
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
