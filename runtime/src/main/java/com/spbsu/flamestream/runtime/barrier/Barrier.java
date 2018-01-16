package com.spbsu.flamestream.runtime.barrier;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.barrier.api.AttachRear;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.ArrayList;
import java.util.List;

public class Barrier extends LoggingActor {
  private final ActorRef acker;
  private final InvalidatingBucket invalidatingBucket = new ArrayInvalidatingBucket();

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
              invalidatingBucket.insert(item);
            })
            .match(MinTimeUpdate.class, minTimeUpdate -> {
              final int pos = invalidatingBucket.lowerBound(new Meta(minTimeUpdate.minTime()));
              invalidatingBucket.rangeStream(0, pos).forEach(di -> rears.forEach(rear -> rear.tell(di, self())));
              invalidatingBucket.clearRange(0, pos);
            })
            .match(AttachRear.class, attach -> {
              log().info("Attach rear request: {}", attach.rear());
              rears.add(attach.rear());
            })
            .build();
  }
}
