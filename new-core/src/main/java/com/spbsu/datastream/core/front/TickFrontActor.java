package com.spbsu.datastream.core.front;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.AckerReport;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.node.UnresolvedMessage;
import com.spbsu.datastream.core.range.HashedMessage;
import com.spbsu.datastream.core.range.atomic.PortBindDataItem;
import com.spbsu.datastream.core.tick.TickInfo;
import com.spbsu.datastream.core.tick.TickMessage;
import com.sun.xml.internal.rngom.digested.DInterleavePattern;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

final class TickFrontActor extends LoggingActor {
  private final ActorRef dns;
  private final InPort target;
  private final int frontId;

  private final TickInfo tickInfo;

  private Cancellable pingMe;

  public static Props props(final ActorRef dns,
                            final InPort target,
                            final int frontId,
                            final TickInfo info) {
    return Props.create(TickFrontActor.class, dns, target, frontId, info);
  }

  private TickFrontActor(final ActorRef dns,
                         final InPort target,
                         final int frontId,
                         final TickInfo info) {
    this.dns = dns;
    this.target = target;
    this.frontId = frontId;
    this.tickInfo = info;

    this.currentWindowHead = this.tickInfo.startTs();
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    final FiniteDuration start = Duration.create(Math.max(this.tickInfo.startTs() - System.nanoTime(), 0), TimeUnit.NANOSECONDS);

    this.pingMe = this.context().system().scheduler().schedule(
            start,
            FiniteDuration.apply(this.tickInfo.window(), TimeUnit.NANOSECONDS),
            this.self(),
            "REMIND YOUR PARENT TO PING YOU",
            this.context().system().dispatcher(),
            ActorRef.noSender()
    );
  }

  @Override
  public void postStop() throws Exception {
    super.postStop();
    this.pingMe.cancel();
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(DataItem.class, this::dispatchItem)
            .match(String.class, m -> this.context().parent().tell("PING ME", this.self()))
            .match(Long.class, this::processPing).build();
  }

  private void processPing(final long ping) {
    if (ping >= this.tickInfo.stopTs()) {
      this.reportUpTo(this.tickInfo.stopTs());
      this.context().stop(this.self());
    } else if (ping >= this.currentWindowHead + this.tickInfo.window()) {
      this.reportUpTo(this.lower(ping));
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void dispatchItem(final DataItem<?> item) {
    final HashFunction hashFunction = this.target.hashFunction();
    final int hash = hashFunction.applyAsInt(item.payload());

    final int receiver = this.tickInfo.hashMapping().entrySet().stream().filter(e -> e.getKey().contains(hash))
            .map(Map.Entry::getValue).findAny().orElseThrow(NoSuchElementException::new);

    final UnresolvedMessage<TickMessage<HashedMessage<PortBindDataItem>>> message = new UnresolvedMessage<>(receiver,
            new TickMessage<>(this.tickInfo.startTs(),
                    new HashedMessage<>(hash,
                            new PortBindDataItem(item, this.target))));

    this.dns.tell(message, ActorRef.noSender());

    this.report(item.meta().globalTime().time(), item.ack());
  }

  private long currentWindowHead;
  private long currentXor = 0;

  private long lower(final long ts) {
    return this.tickInfo.startTs() + this.tickInfo.window() * ((ts - this.tickInfo.startTs()) / this.tickInfo.window());
  }

  private void report(final long time, final long xor) {
    if (time >= this.currentWindowHead + this.tickInfo.window()) {
      this.reportUpTo(this.lower(time));
    }
    this.currentXor ^= xor;
  }

  private void reportUpTo(final long windowHead) {
    for (; this.currentWindowHead < windowHead; this.currentWindowHead += this.tickInfo.window(), this.currentXor = 0) {
      this.closeWindow(this.currentWindowHead, this.currentXor);
    }
  }

  private void closeWindow(final long windowHead, final long xor) {
    final AckerReport report = new AckerReport(new GlobalTime(windowHead, this.frontId), xor);
    this.LOG().debug("Closing window {}", report);
    final UnresolvedMessage<TickMessage<AckerReport>> message = new UnresolvedMessage<>(this.tickInfo.ackerLocation(),
            new TickMessage<>(this.tickInfo.startTs(),
                    report));

    this.dns.tell(message, ActorRef.noSender());
  }
}
