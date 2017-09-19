package com.spbsu.datastream.core.front;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.meta.GlobalTime;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.meta.Meta;
import com.spbsu.datastream.core.tick.TickInfo;

import java.util.TreeMap;

public final class FrontActor extends LoggingActor {
  private final ActorRef dns;
  private final int id;
  private final TreeMap<Long, ActorRef> tickFronts = new TreeMap<>();

  private long prevGlobalTs = -1;

  public static Props props(ActorRef dns, int id) {
    return Props.create(FrontActor.class, dns, id);
  }

  private FrontActor(ActorRef dns, int id) {
    this.dns = dns;
    this.id = id;
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder()
            .match(RawData.class, this::redirectItem)
            .match(TickInfo.class, this::createTick)
            .match(String.class, this::onPing).build();
  }

  private void onPing(String ping) {
    this.sender().tell(System.nanoTime(), this.self());
  }


  private void createTick(TickInfo tickInfo) {
    this.LOG().info("Creating tickFront for startTs: {}", tickInfo);

    final InPort target = tickInfo.graph().frontBindings().get(this.id);

    final ActorRef tickFront = this.context().actorOf(TickFrontActor.props(this.dns,
            target,
            this.id,
            tickInfo),
            Long.toString(tickInfo.startTs()));

    this.tickFronts.put(tickInfo.startTs(), tickFront);
  }

  private void redirectItem(RawData<?> data) {
    long globalTs = System.nanoTime();
    if (globalTs <= prevGlobalTs) {
      globalTs = prevGlobalTs + 1;
    }
    prevGlobalTs = globalTs;

    final GlobalTime globalTime = new GlobalTime(globalTs, this.id);
    final Meta now = Meta.meta(globalTime);
    final DataItem<?> dataItem = new PayloadDataItem<>(now, data.payload());

    final long tick = this.tickFronts.floorKey(globalTime.time());

    final ActorRef tickFront = this.tickFronts.get(tick);
    tickFront.tell(dataItem, this.self());
  }
}












