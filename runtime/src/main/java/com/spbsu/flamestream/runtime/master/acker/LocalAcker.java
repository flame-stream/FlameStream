package com.spbsu.flamestream.runtime.master.acker;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.graph.Joba;
import com.spbsu.flamestream.runtime.master.acker.api.Ack;
import com.spbsu.flamestream.runtime.master.acker.api.AckerInputMessage;
import com.spbsu.flamestream.runtime.master.acker.api.JobaTime;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.akka.PingActor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class LocalAcker extends LoggingActor {
  private static final int FLUSH_DELAY_IN_MILLIS = 1;
  private static final int FLUSH_COUNT = 100;

  private final HashMap<Joba.Id, Long> jobaTimesCache = new HashMap<>();
  private final SortedMap<GlobalTime, Long> ackCache = new TreeMap<>(Comparator.reverseOrder());
  private final List<AckerInputMessage> fifoCache = new ArrayList<>();

  private final ActorRef globalAcker;
  private final ActorRef pingActor;

  private int flushCounter = 0;

  public LocalAcker(ActorRef globalAcker) {
    this.globalAcker = globalAcker;
    pingActor = context().actorOf(PingActor.props(self(), Flush.FLUSH));
  }

  public static Props props(ActorRef globalAcker) {
    return Props.create(LocalAcker.class, globalAcker).withDispatcher("processing-dispatcher");
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
            .match(JobaTime.class, this::handleJobaTime)
            .match(Ack.class, this::handleAck)
            .match(Flush.class, flush -> flush())
            .match(AckerInputMessage.class, fifoCache::add)
            .matchAny(e -> globalAcker.forward(e, context()))
            .build();
  }

  private void tick() {
    if (flushCounter == FLUSH_COUNT) {
      flush();
    } else {
      flushCounter++;
    }
  }

  private void handleJobaTime(JobaTime jobaTime) {
    jobaTimesCache.compute(jobaTime.jobaId, (__, value) -> {
      if (value == null) {
        return jobaTime.time;
      } else {
        return Math.max(jobaTime.time, value);
      }
    });

    tick();
  }

  private void handleAck(Ack ack) {
    ackCache.compute(ack.time(), (globalTime, xor) -> {
      if (xor == null) {
        return ack.xor();
      } else {
        return ack.xor() ^ xor;
      }
    });

    tick();
  }

  private void flush() {
    jobaTimesCache.forEach((jobaId, value) -> globalAcker.tell(new JobaTime(jobaId, value), context().parent()));
    jobaTimesCache.clear();

    ackCache.forEach((globalTime, xor) -> globalAcker.tell(new Ack(globalTime, xor), context().parent()));
    ackCache.clear();

    fifoCache.forEach(heartbeat -> globalAcker.tell(heartbeat, self()));
    fifoCache.clear();

    flushCounter = 0;
  }

  private enum Flush {
    FLUSH
  }
}
