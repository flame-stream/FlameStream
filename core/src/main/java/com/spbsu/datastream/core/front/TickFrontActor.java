package com.spbsu.datastream.core.front;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.AckerReport;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.meta.GlobalTime;
import com.spbsu.datastream.core.range.AddressedItem;
import com.spbsu.datastream.core.tick.HashMapping;
import com.spbsu.datastream.core.tick.TickInfo;
import com.spbsu.datastream.core.tick.TickRoutes;
import com.spbsu.datastream.core.tick.TickRoutesResolver;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.ToIntFunction;

import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toMap;

final class TickFrontActor extends LoggingActor {
  private final Map<Integer, ActorPath> tickConcierges;
  private final InPort target;
  private final int frontId;
  private final TickInfo tickInfo;

  @Nullable
  private TickRoutes routes;

  @Nullable
  private HashMapping<ActorRef> mapping;

  private Cancellable pingMe;

  private TickFrontActor(Map<Integer, ActorPath> cluster,
                         InPort target,
                         int frontId,
                         TickInfo info) {
    this.target = target;
    this.frontId = frontId;
    this.tickInfo = info;

    this.tickConcierges = cluster.entrySet().stream()
            .collect(toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().child(String.valueOf(tickInfo.startTs()))
            ));

    this.currentWindowHead = tickInfo.startTs();
  }

  public static Props props(Map<Integer, ActorPath> cluster,
                            InPort target,
                            int frontId,
                            TickInfo info) {
    return Props.create(TickFrontActor.class, cluster, target, frontId, info);
  }

  @Override
  public void preStart() throws Exception {
    context().actorOf(TickRoutesResolver.props(tickConcierges, tickInfo), "resolver");
    super.preStart();
  }

  @Override
  public void postStop() {
    if (pingMe != null) {
      pingMe.cancel();
    }
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(TickRoutes.class, routes -> {
              this.routes = routes;
              mapping = HashMapping.hashMapping(routes.rangeConcierges());

              final FiniteDuration start = Duration.create(max(tickInfo.startTs() - System.nanoTime(), 0), NANOSECONDS);
              this.pingMe = context().system().scheduler().schedule(
                      start,
                      FiniteDuration.apply(tickInfo.window(), NANOSECONDS),
                      self(),
                      "REMIND YOUR PARENT TO PING YOU",
                      context().system().dispatcher(),
                      self()
              );

              getContext().become(receiving());
              unstashAll();
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive receiving() {
    return receiveBuilder()
            .match(DataItem.class, this::dispatchItem)
            .match(String.class, m -> context().parent().tell("PING ME", self()))
            .match(Long.class, this::processPing).build();
  }

  private void processPing(long ping) {
    if (ping >= tickInfo.stopTs()) {
      reportUpTo(tickInfo.stopTs());
      context().stop(self());
    } else if (ping >= currentWindowHead + tickInfo.window()) {
      reportUpTo(lower(ping));
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void dispatchItem(DataItem<?> item) {
    final ToIntFunction hashFunction = target.hashFunction();
    final int hash = hashFunction.applyAsInt(item.payload());

    final ActorRef receiver = mapping.valueFor(hash);
    final AddressedItem message = new AddressedItem(item, target);
    receiver.tell(message, self());

    report(item.meta().globalTime().time(), item.ack());
  }

  private long currentWindowHead;
  private long currentXor = 0;

  private long lower(long ts) {
    return tickInfo.startTs() + tickInfo.window() * ((ts - tickInfo.startTs()) / tickInfo.window());
  }

  private void report(long time, long xor) {
    if (time >= currentWindowHead + tickInfo.window()) {
      reportUpTo(lower(time));
    }
    this.currentXor ^= xor;
  }

  private void reportUpTo(long windowHead) {
    for (; currentWindowHead < windowHead; this.currentWindowHead += tickInfo.window(), this.currentXor = 0) {
      closeWindow(currentWindowHead, currentXor);
    }
  }

  private void closeWindow(long windowHead, long xor) {
    final AckerReport report = new AckerReport(new GlobalTime(windowHead, frontId), xor);
    routes.acker().tell(report, self());
  }
}
