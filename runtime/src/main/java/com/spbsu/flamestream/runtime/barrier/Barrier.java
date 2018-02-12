package com.spbsu.flamestream.runtime.barrier;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.barrier.api.AttachRear;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.ArrayList;
import java.util.List;

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
              //acker.tell(new Ack(item.meta().globalTime(), item.xor()), self());
              collector.enqueue(item);
            })
            .match(MinTimeUpdate.class, minTimeUpdate -> {
              final GlobalTime globalTime = minTimeUpdate.minTime();
              collector.releaseFrom(globalTime, di -> rears.forEach(rear -> rear.tell(di, self())));
            })
            .match(AttachRear.class, attach -> {
              log().info("Attach rear request: {}", attach.rear());
              rears.add(attach.rear());
            })
            .build();
  }
}
