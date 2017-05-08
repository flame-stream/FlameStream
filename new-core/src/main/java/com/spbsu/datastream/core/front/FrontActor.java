package com.spbsu.datastream.core.front;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.tick.TickInfo;

import java.util.TreeMap;

public final class FrontActor extends LoggingActor {
  private final ActorRef dns;
  private final int id;

  private final TreeMap<Long, ActorRef> tickFronts = new TreeMap<>();

  public static Props props(final ActorRef dns, final int id) {
    return Props.create(FrontActor.class, dns, id);
  }

  private FrontActor(final ActorRef dns, final int id) {
    this.dns = dns;
    this.id = id;
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG().debug("Received: {}", message);
    if (message instanceof RawData) {
      final Object data = ((RawData<?>) message).payload();
      this.redirectItem(data);
    } else if (message instanceof TickInfo) {
      final TickInfo info = (TickInfo) message;
      this.createTick(info);
    } else if (message instanceof String) {
      this.sender().tell(System.nanoTime(), ActorRef.noSender());
    } else {
      this.unhandled(message);
    }
  }

  private void createTick(final TickInfo tickInfo) {
    this.LOG().info("Deploying for startTs: {}", tickInfo);

    final InPort target = tickInfo.graph().frontBindings().get(this.id);

    final ActorRef tickFront = this.context().actorOf(TickFrontActor.props(this.dns,
            target,
            this.id,
            tickInfo),
            Long.toString(tickInfo.startTs()));

    this.tickFronts.put(tickInfo.startTs(), tickFront);
  }

  private void redirectItem(final Object data) {
    final GlobalTime globalTime = new GlobalTime(System.nanoTime(), this.id);
    final Meta now = new Meta(globalTime);
    final DataItem<?> dataItem = new PayloadDataItem<>(now, data);

    final long tick = this.tickFronts.floorKey(globalTime.time());

    final ActorRef tickFront = this.tickFronts.get(tick);
    tickFront.tell(dataItem, ActorRef.noSender());
  }
}












