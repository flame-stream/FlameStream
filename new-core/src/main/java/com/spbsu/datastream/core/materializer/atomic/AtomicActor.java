package com.spbsu.datastream.core.materializer.atomic;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.materializer.AddressedMessage;
import scala.Option;

import static com.spbsu.datastream.core.materializer.manager.TickGraphManagerApi.TickStarted;

public class AtomicActor extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

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
    LOG.info("Starting...");
  }

  @Override
  public void postStop() throws Exception {
    LOG.info("Stopped");
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    LOG.debug("Received {}", message);

    if (message instanceof AddressedMessage) {
      handleAddressedMessage((AddressedMessage) message);

    } else if (message instanceof TickStarted) {
      atomic.onStart(handle);

    } else {
      unhandled(message);
    }
  }

  private void handleAddressedMessage(final AddressedMessage message) {
    Object payload = message.payload();
    if (payload instanceof DataItem) {
      atomic.onPush(message.port(), (DataItem<?>) payload, handle);
    }
  }
}
