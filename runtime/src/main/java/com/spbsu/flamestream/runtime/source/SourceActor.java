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
    return super.createReceive().orElse(
            ReceiveBuilder.create()
                    .match(
                            DataItem.class,
                            dataItem -> dataItem.meta().globalTime().time() >= tickInfo.stopTs(),
                            dataItem -> {
                              sender().tell(new PleaseWait(42), self()); // TODO: 10.11.2017 think about magic number
                            }
                    )
                    .match(
                            DataItem.class,
                            dataItem -> dataItem.meta().globalTime().time() >= tickInfo.startTs()
                                    && dataItem.meta().globalTime().time() < tickInfo.stopTs(),
                            dataItem -> {
                              sourceHandle.putRef(dataItem.meta().globalTime().front(), sender());
                              source.onNext(dataItem, sourceHandle);
                            }
                    )
                    .match(Heartbeat.class, heartbeat -> source.onHeartbeat(heartbeat.time(), sourceHandle))
                    .build()
    );
  }

  @Override
  protected void onMinTimeUpdate(MinTimeUpdate message) {
    source.onMinGTimeUpdate(message.minTime(), sourceHandle);
  }
}
