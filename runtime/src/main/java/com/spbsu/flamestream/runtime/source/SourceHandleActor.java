package com.spbsu.flamestream.runtime.source;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.graph.source.SourceHandle;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.source.messages.Start;

public final class SourceHandleActor extends LoggingActor {
  private final SourceHandle handle;
  private ActorRef front;

  private SourceHandleActor(SourceHandle handle) {
    this.handle = handle;
    resolveFront();
  }

  public static Props props(SourceHandle sourceHandle) {
    return Props.create(SourceHandleActor.class, sourceHandle);
  }

  @Override
  public void preStart() {
    super.aroundPreStart();
    front.tell(new Start(), self());
  }

  private void resolveFront() {
  }

  public static Props props(AtomicHandle handle, OutPort outPort) {
    return Props.create(SourceHandleActor.class, handle, outPort);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(DataItem.class, i -> handle.onInput(i))
            .match(Exception.class, e -> handle.onError(e))
            .match(GlobalTime.class, w -> handle.onWatermark(w))
            .build();
  }
}
