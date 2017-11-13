package com.spbsu.flamestream.runtime.source;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.source.Source;
import com.spbsu.flamestream.runtime.ack.messages.MinTimeUpdate;
import com.spbsu.flamestream.runtime.range.atomic.AtomicActor;
import com.spbsu.flamestream.runtime.source.api.Heartbeat;
import com.spbsu.flamestream.runtime.source.api.NewHole;
import com.spbsu.flamestream.runtime.source.api.PleaseWait;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.spbsu.flamestream.runtime.tick.TickRoutes;

/**
 * User: Artem
 * Date: 10.11.2017
 */
public class SourceActor extends AtomicActor {
  private final Source source;
  private final TickInfo tickInfo;
  private final SourceHandleImpl sourceHandle;

  private SourceActor(Source source, TickInfo tickInfo, TickRoutes tickRoutes) {
    super(source, tickInfo, tickRoutes);
    this.source = source;
    this.tickInfo = tickInfo;
    sourceHandle = new SourceHandleImpl(tickInfo, tickRoutes, context());
  }

  public static Props props(Source source, TickInfo tickInfo, TickRoutes tickRoutes) {
    return Props.create(SourceActor.class, source, tickInfo, tickRoutes);
  }

  @Override
  public void preStart() throws Exception {
    context().actorSelection("/user/watcher/concierge/fronts/*")
            .tell(new NewHole(self(), tickInfo.startTs()), self());
    super.preStart();
  }

  @Override
  public Receive createReceive() {
    //noinspection unchecked
    return ReceiveBuilder.create()
            .match(DataItem.class, dataItem -> {
              final long time = dataItem.meta().globalTime().time();
              if (time < tickInfo.startTs()) {
                throw new IllegalStateException("DataItems ts cannot be less than tick start");
              } else if (time >= tickInfo.stopTs()) {
                sender().tell(new PleaseWait(42), self()); // TODO: 10.11.2017 think about magic number
              }
              sourceHandle.putRef(dataItem.meta().globalTime().front(), sender());
              source.onNext(dataItem, sourceHandle);
            })
            .match(Heartbeat.class, heartbeat -> {
              final long time = heartbeat.time().time();
              if (time >= tickInfo.startTs() && time < tickInfo.stopTs()) {
                source.onHeartbeat(heartbeat.time(), sourceHandle);
              }
            })
            .matchAny(super.createReceive())
            .build();
    );
  }

  @Override
  protected void onMinTimeUpdate(MinTimeUpdate message) {
    source.onMinGTimeUpdate(message.minTime(), sourceHandle);
    super.onMinTimeUpdate(message);
  }
}
