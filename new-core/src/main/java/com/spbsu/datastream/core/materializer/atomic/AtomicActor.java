package com.spbsu.datastream.core.materializer.atomic;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.materializer.AddressedMessage;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import scala.Option;

import java.util.Collection;

import static com.spbsu.datastream.core.materializer.manager.TickGraphManagerApi.TickStarted;

public class AtomicActor extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

  private final AtomicGraph atomic;
  private final TLongObjectMap<InPort> portMappings;
  private final AtomicHandle handle;
  private final String id;

  private AtomicActor(final AtomicGraph atomic, final AtomicHandle handle, final String id) {
    this.atomic = atomic;
    this.handle = handle;
    this.portMappings = idMappings(atomic.inPorts());
    this.id = id;
  }

  public static Props props(final AtomicGraph atomic, final AtomicHandle handle, final String id) {
    return Props.create(AtomicActor.class, atomic, handle, id);
  }

  private TLongObjectMap<InPort> idMappings(Collection<InPort> inPorts) {
    final TLongObjectMap<InPort> result = new TLongObjectHashMap<>();
    for (final InPort port : inPorts) {
      result.put(port.id(), port);
    }
    return result;
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    LOG.debug("Received {}", message);

    if (message instanceof AddressedMessage) {
      onAddressedMessage((AddressedMessage) message);
    } else if (message instanceof TickStarted) {
      atomic.onStart(handle);
    } else {
      unhandled(message);
    }
  }

  @Override
  public void preStart() throws Exception {
    LOG.info("Starting...");
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    LOG.info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  private void onAddressedMessage(final AddressedMessage message) {
    atomic.onPush(portMappings.get(message.port()), message.payload(), handle);
  }
}
