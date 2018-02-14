package com.spbsu.flamestream.runtime.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.RegisterFront;
import com.spbsu.flamestream.runtime.acker.api.UnregisterFront;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.akka.PingActor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class LocalAcker extends LoggingActor {
  private static final int FLUSH_DELAY_IN_MILLIS = 1;
  private static final int FLUSH_COUNT = 10;

  private final SortedMap<GlobalTime, Long> ackCache = new TreeMap<>(Comparator.reverseOrder());
  private final List<Heartbeat> heartbeatCache = new ArrayList<>();

  private final ActorRef globalAcker;
  private final ActorRef pingActor;

  private int flushCounter = 0;

  public LocalAcker(ActorRef globalAcker) {
    this.globalAcker = globalAcker;
    pingActor = context().actorOf(PingActor.props(self(), Flush.FLUSH).withDispatcher("resolver-dispatcher"));
  }

  public static Props props(ActorRef globalAcker) {
    return Props.create(LocalAcker.class, globalAcker);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    pingActor.tell(new PingActor.Start(TimeUnit.MILLISECONDS.toNanos(FLUSH_DELAY_IN_MILLIS)), self());
  }

  @Override
  public void postStop() {
    pingActor.tell(new PingActor.Stop(), self());
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Ack.class, this::handleAck)
            .match(Heartbeat.class, heartbeatCache::add)
            .match(RegisterFront.class, registerFront -> globalAcker.forward(registerFront, context()))
            .match(UnregisterFront.class, unregisterFront -> globalAcker.forward(unregisterFront, context()))
            .match(Flush.class, flush -> flush())
            .build();
  }

  private void handleAck(Ack ack) {
    ackCache.compute(ack.time(), (globalTime, xor) -> {
      if (xor == null) {
        return ack.xor();
      } else {
        return ack.xor() ^ xor;
      }
    });

    if (flushCounter == FLUSH_COUNT) {
      flush();
    } else {
      flushCounter++;
    }
  }

  private void flush() {
    ackCache.forEach((globalTime, xor) -> globalAcker.tell(new Ack(globalTime, xor), context().parent()));
    ackCache.clear();

    heartbeatCache.forEach(heartbeat -> globalAcker.tell(heartbeat, self()));
    heartbeatCache.clear();

    flushCounter = 0;
  }

  private enum Flush {
    FLUSH
  }
}
