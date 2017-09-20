package com.spbsu.datastream.core.front;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import com.spbsu.datastream.core.message.AckerMessage;
import com.spbsu.datastream.core.message.AtomicMessage;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.meta.GlobalTime;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.AckerReport;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.node.UnresolvedMessage;
import com.spbsu.datastream.core.tick.TickInfo;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;

final class TickFrontActor extends LoggingActor {
  private final ActorRef dns;
  private final InPort target;
  private final int frontId;

  private final TickInfo tickInfo;

  private Cancellable pingMe;

  public static Props props(ActorRef dns,
                            InPort target,
                            int frontId,
                            TickInfo info) {
    return Props.create(TickFrontActor.class, dns, target, frontId, info);
  }

  private TickFrontActor(ActorRef dns,
                         InPort target,
                         int frontId,
                         TickInfo info) {
    this.dns = dns;
    this.target = target;
    this.frontId = frontId;
    this.tickInfo = info;

    this.currentWindowHead = tickInfo.startTs();
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    final FiniteDuration start = Duration.create(Math.max(tickInfo.startTs() - System.nanoTime(), 0), TimeUnit.NANOSECONDS);

    this.pingMe = context().system().scheduler().schedule(
            start,
            FiniteDuration.apply(tickInfo.window(), TimeUnit.NANOSECONDS),
            self(),
            "REMIND YOUR PARENT TO PING YOU",
            context().system().dispatcher(),
            self()
    );
  }

  @Override
  public void postStop() throws Exception {
    super.postStop();
    pingMe.cancel();
  }

  @Override
  public Receive createReceive() {
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

    final int receiver = tickInfo.hashMapping().workerForHash(hash);

    final UnresolvedMessage<AtomicMessage<?>> message = new UnresolvedMessage<>(receiver,
            new AtomicMessage<>(tickInfo.startTs(), hash, target, item));

    dns.tell(message, self());

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
    LOG().debug("Closing window {}", report);
    final UnresolvedMessage<AckerMessage<AckerReport>> message = new UnresolvedMessage<>(
            tickInfo.ackerLocation(),
            new AckerMessage<>(report, tickInfo.startTs()));

    dns.tell(message, self());
  }
}
