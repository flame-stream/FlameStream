package com.spbsu.datastream.core.front;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.FrontReport;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.range.AddressedMessage;
import com.spbsu.datastream.core.tick.PortBindDataItem;

final class TickFrontActor extends LoggingActor {
  private final ActorRef rootRouter;
  private final InPort target;
  private final int frontId;
  private final HashRange ackLocation;

  private final long tick;
  private final long window;
  private final long startTs;


  public static Props props(final ActorRef rootRouter,
                            final HashRange ackLocation,
                            final InPort target,
                            final int frontId,
                            final long tick,
                            final long window) {
    return Props.create(TickFrontActor.class, rootRouter, ackLocation, target, frontId, tick, window);
  }

  private TickFrontActor(final ActorRef rootRouter,
                         final HashRange ackLocation,
                         final InPort target,
                         final int frontId,
                         final long tick,
                         final long window) {
    this.rootRouter = rootRouter;
    this.ackLocation = ackLocation;
    this.target = target;
    this.frontId = frontId;
    this.tick = tick;
    this.window = window;

    this.startTs = tick;
    this.currentWindowHead = this.startTs;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof DataItem) {
      final DataItem<?> item = (DataItem<?>) message;

      final HashFunction hashFunction = this.target.hashFunction();
      final int hash = hashFunction.applyAsInt(item.payload());

      // TODO: 4/28/17 bookkeeping
      this.rootRouter.tell(new AddressedMessage<>(new PortBindDataItem(item, this.target), hash, this.tick),
              ActorRef.noSender());

      this.report(item.meta().globalTime().time(), item.ack());
    } else if (message instanceof Long) {
      final long currentTime = (long) message;
      if (currentTime > this.currentWindowHead + this.window) {
        this.reportUpTo(this.lower(currentTime));
      }
    }
  }

  private long currentWindowHead;
  private long currentXor = 0;

  private long lower(final long ts) {
    return this.startTs + this.window * ((ts - this.startTs) / this.window);
  }


  private void report(final long time, final long xor) {
    if (time >= this.currentWindowHead + this.window) {
      this.reportUpTo(this.lower(time));
    }
    this.currentXor ^= xor;
  }

  private void reportUpTo(final long windowHead) {
    for (; this.currentWindowHead < windowHead; this.currentWindowHead += this.window, this.currentXor = 0) {
      this.closeWindow(this.currentWindowHead, this.currentXor);
    }
  }

  private void closeWindow(final long windowHead, final long xor) {
    final FrontReport report = new FrontReport(new GlobalTime(windowHead, this.frontId), xor);
    this.rootRouter.tell(new AddressedMessage<>(report, this.ackLocation.from(), this.tick), ActorRef.noSender());
  }
}
