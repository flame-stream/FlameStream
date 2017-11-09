package com.spbsu.flamestream.runtime.front;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.runtime.ack.messages.Ack;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.range.AddressedItem;
import com.spbsu.flamestream.runtime.tick.HashMapping;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.spbsu.flamestream.runtime.tick.TickRoutes;
import com.spbsu.flamestream.runtime.tick.TickRoutesResolver;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.ToIntFunction;

import static java.util.stream.Collectors.toMap;

final class TickFrontActor extends LoggingActor {
  private final Map<Integer, ActorPath> tickConcierges;
  private final InPort target;
  private final int frontId;
  private final TickInfo tickInfo;

  @Nullable
  private TickRoutes routes = null;
  @Nullable
  private HashMapping<ActorRef> mapping = null;

  private long currentWindowHead;
  private long currentXor = 0;

  private TickFrontActor(Map<Integer, ActorPath> cluster, InPort target, int frontId, TickInfo info) {
    this.target = target;
    this.frontId = frontId;
    this.tickInfo = info;

    this.tickConcierges = cluster.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, e -> e.getValue().child(String.valueOf(tickInfo.id()))));
    this.currentWindowHead = tickInfo.startTs();
  }

  public static Props props(Map<Integer, ActorPath> cluster, InPort target, int frontId, TickInfo info) {
    return Props.create(TickFrontActor.class, cluster, target, frontId, info);
  }

  @Override
  public void preStart() throws Exception {
    context().actorOf(TickRoutesResolver.props(tickConcierges, tickInfo), "resolver");
    super.preStart();
  }

  @Override
  public void postStop() {
    context().parent().tell(new TickFrontStopped(tickInfo.startTs()), self());
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create().match(TickRoutes.class, routes -> {
      this.routes = routes;
      mapping = HashMapping.hashMapping(routes.rangeConcierges());
      getContext().become(receiving());
      unstashAll();
    }).matchAny(m -> stash()).build();
  }

  private Receive receiving() {
    return ReceiveBuilder.create()
            .match(DataItem.class, this::dispatchItem)
            .match(TickFrontPing.class, this::processTsResponse)
            .matchAny(this::unhandled)
            .build();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void dispatchItem(DataItem<?> item) {
    final ToIntFunction hashFunction = target.hashFunction();
    final int hash = hashFunction.applyAsInt(item.payload());

    //noinspection ConstantConditions
    final ActorRef receiver = mapping.valueFor(hash);
    final AddressedItem message = new AddressedItem(item, target);
    receiver.tell(message, self());

    { //report
      long time = item.meta().globalTime().time();
      if (time >= currentWindowHead + tickInfo.window()) {
        for (; currentWindowHead < lower(time); this.currentWindowHead += tickInfo.window(), this.currentXor = 0) {
          final Ack report = new Ack(new GlobalTime(currentWindowHead, frontId), currentXor, true);
          //noinspection ConstantConditions
          routes.acker().tell(report, self());
        }
      }
      this.currentXor ^= item.ack();
    }
  }

  private void processTsResponse(TickFrontPing tickFrontPing) {
    final long ts = tickFrontPing.ts();
    if (ts >= tickInfo.stopTs()) {
      reportUpTo(tickInfo.stopTs());
      context().stop(self());
    } else if (ts >= currentWindowHead + tickInfo.window()) {
      reportUpTo(lower(ts));
    }
  }

  private void reportUpTo(long windowHead) {
    for (; currentWindowHead < windowHead; this.currentWindowHead += tickInfo.window(), this.currentXor = 0) {
      final Ack report = new Ack(new GlobalTime(currentWindowHead, frontId), currentXor, true);
      //noinspection ConstantConditions
      routes.acker().tell(report, self());
    }
  }

  private long lower(long ts) {
    return tickInfo.startTs() + tickInfo.window() * ((ts - tickInfo.startTs()) / tickInfo.window());
  }
}
