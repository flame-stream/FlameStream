package com.spbsu.flamestream.runtime.node.tick.range.atomic.source;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.source.Source;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.node.tick.api.TickInfo;
import com.spbsu.flamestream.runtime.node.tick.api.TickRoutes;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.AtomicActor;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api.Heartbeat;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api.NewHole;
import com.spbsu.flamestream.runtime.node.tick.range.atomic.source.api.PleaseWait;

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
                    .match(DataItem.class, dataItem -> {
                      if (tickInfo.isInTick(dataItem.meta().globalTime())) {
                        sourceHandle.putRef(dataItem.meta().globalTime().front(), sender());
                        source.onNext(dataItem, sourceHandle);
                      } else {
                        sender().tell(new PleaseWait(42), self());
                      }
                    })
                    .match(Heartbeat.class, heartbeat -> source.onHeartbeat(heartbeat.time(), sourceHandle))
                    .build()
    );
  }

  @Override
  protected void onMinTimeUpdate(MinTimeUpdate message) {
    source.onMinGTimeUpdate(message.minTime(), sourceHandle);
  }
}
