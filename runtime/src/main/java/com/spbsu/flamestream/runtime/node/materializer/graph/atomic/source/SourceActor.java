package com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.source.Source;
import com.spbsu.flamestream.runtime.node.materializer.GraphRoutes;
import com.spbsu.flamestream.runtime.node.materializer.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.node.materializer.graph.atomic.AtomicActor;
import com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source.api.Heartbeat;
import com.spbsu.flamestream.runtime.node.materializer.graph.atomic.source.api.NewHole;

/**
 * User: Artem
 * Date: 10.11.2017
 */
public class SourceActor extends AtomicActor {
  private final Source source;
  private final SourceHandleImpl sourceHandle;

  private SourceActor(Source source, GraphRoutes routes) {
    super(source, routes);
    this.source = source;
    sourceHandle = new SourceHandleImpl(routes, context());
  }

  public static Props props(Source source, GraphRoutes routes) {
    return Props.create(SourceActor.class, source, routes);
  }

  @Override
  public void preStart() throws Exception {
    context().actorSelection("/user/watcher/tickrouter")
            .tell(new NewHole(self()), self());
    super.preStart();
  }

  @Override
  public Receive createReceive() {
    //noinspection unchecked
    return super.createReceive().orElse(
            ReceiveBuilder.create()
                    .match(DataItem.class, dataItem -> {
                      sourceHandle.putRef(dataItem.meta().globalTime().front(), sender());
                      source.onNext(dataItem, sourceHandle);
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
