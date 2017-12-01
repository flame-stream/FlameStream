package com.spbsu.flamestream.runtime.barrier;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.barrier.api.AttachRear;
import com.spbsu.flamestream.runtime.utils.Statistics;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import gnu.trove.impl.Constants;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;

public class Barrier extends LoggingActor {
  private final ActorRef acker;
  private final BarrierCollector collector = new BarrierCollector();

  private final List<ActorRef> rears = new ArrayList<>();

  private Barrier(ActorRef acker) {
    this.acker = acker;
  }

  public static Props props(ActorRef acker) {
    return Props.create(Barrier.class, acker);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(DataItem.class, item -> {
              acker.tell(new Ack(item.meta().globalTime(), item.xor()), self());
              collector.enqueue(item);
            })
            .match(MinTimeUpdate.class, minTimeUpdate -> {
              final GlobalTime globalTime = minTimeUpdate.minTime();
              collector.releaseFrom(globalTime, di -> {
                final Object data = di.payload();
                rears.forEach(rear -> rear.tell(data, self()));
                //barrierStatistics.release(di.meta().time());
              });
            })
            .match(AttachRear.class, attach -> {
              log().info("Attach rear request: {}", attach.rear());
              rears.add(attach.rear());
            })
            .build();
  }


  private static class BarrierStatistics implements Statistics {
    private final TObjectLongMap<GlobalTime> timeMeasure = new TObjectLongHashMap<>();
    private final LongSummaryStatistics duration = new LongSummaryStatistics();

    void enqueue(GlobalTime globalTime) {
      timeMeasure.put(globalTime, System.nanoTime());
    }

    void release(GlobalTime globalTime) {
      final long start = timeMeasure.get(globalTime);
      if (start != Constants.DEFAULT_LONG_NO_ENTRY_VALUE) {
        duration.accept(System.nanoTime() - start);
        timeMeasure.remove(globalTime);
      }
    }

    @Override
    public Map<String, Double> metrics() {
      return Statistics.asMap("Barrier releasing duration", duration);
    }

    @Override
    public String toString() {
      return metrics().toString();
    }
  }
}
