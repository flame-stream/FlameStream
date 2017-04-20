package com.spbsu.datastream.core.tick.atomic;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.tick.AddressedMessage;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import scala.Option;

import static com.spbsu.datastream.core.tick.manager.TickGraphManagerApi.TickStarted;

public final class AtomicActor extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());

  private final AtomicGraph atomic;
  private final TLongObjectMap<InPort> portMappings;
  private final AtomicHandle handle;

  private AtomicActor(final AtomicGraph atomic, final AtomicHandle handle, final String id) {
    super();
    this.atomic = atomic;
    this.handle = handle;
    this.portMappings = AtomicActor.portMappings(atomic.inPorts());
    final String id1 = id;
  }

  public static Props props(final AtomicGraph atomic, final AtomicHandle handle, final String id) {
    return Props.create(AtomicActor.class, atomic, handle, id);
  }

  @Override
  public void preStart() throws Exception {
    this.LOG.info("Starting...");
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    this.LOG.info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    this.LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  @SuppressWarnings("ChainOfInstanceofChecks")
  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG.debug("Received {}", message);

    if (message instanceof AddressedMessage) {
      this.onAddressedMessage((AddressedMessage) message);
    } else if (message instanceof TickStarted) {
      this.atomic.onStart(this.handle);
    } else {
      this.unhandled(message);
    }
  }

  private void onAddressedMessage(final AddressedMessage message) {
    this.atomic.onPush(this.portMappings.get(message.port()), message.payload(), this.handle);
  }

  private static TLongObjectMap<InPort> portMappings(final Iterable<InPort> inPorts) {
    final TLongObjectMap<InPort> result = new TLongObjectHashMap<>();
    for (final InPort port : inPorts) {
      result.put(port.id(), port);
    }
    return result;
  }
}
