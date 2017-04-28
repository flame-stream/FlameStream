package com.spbsu.datastream.core.tick.atomic;

import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.tick.PortBindDataItem;

import static com.spbsu.datastream.core.tick.TickConciergeApi.TickStarted;

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

  @SuppressWarnings("ChainOfInstanceofChecks")
  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG.debug("Received {}", message);

    if (message instanceof PortBindDataItem) {
      this.onAddressedMessage((PortBindDataItem) message);
    } else if (message instanceof TickStarted) {
      this.atomic.onStart(this.handle);
    } else {
      this.unhandled(message);
    }
  }

  private void onAddressedMessage(final PortBindDataItem message) {
    this.atomic.onPush(message.inPort(), message.payload(), this.handle);
  }
}
