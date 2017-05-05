package com.spbsu.datastream.core.tick.atomic;

import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.MinTimeUpdate;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.tick.PortBindDataItem;

public final class AtomicActor extends LoggingActor {
  private final AtomicGraph atomic;
  private final AtomicHandle handle;

  private AtomicActor(final AtomicGraph atomic, final AtomicHandle handle) {
    this.atomic = atomic;
    this.handle = handle;
  }

  public static Props props(final AtomicGraph atomic, final AtomicHandle handle) {
    return Props.create(AtomicActor.class, atomic, handle);
  }

  @Override
  public void preStart() throws Exception {
    this.atomic.onStart(this.handle);
    super.preStart();
  }

  @SuppressWarnings("ChainOfInstanceofChecks")
  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG().debug("Received {}", message);

    if (message instanceof PortBindDataItem) {
      this.onAddressedMessage((PortBindDataItem) message);
    } else if (message instanceof MinTimeUpdate) {
      this.onMinTimeUpdate((MinTimeUpdate) message);
    } else {
      this.unhandled(message);
    }
  }

  private void onAddressedMessage(final PortBindDataItem message) {
    this.atomic.onPush(message.inPort(), message.payload(), this.handle);
    this.handle.ack(message.payload());
  }

  private void onMinTimeUpdate(final MinTimeUpdate message) {
    this.atomic.onMinGTimeUpdate(message.minTime(), this.handle);
  }
}
