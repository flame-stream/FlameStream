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

  private final TLongObjectMap<InPort> idPortMapping;

  private final AtomicHandle handle;

  private AtomicActor(final AtomicGraph atomic, final AtomicHandle handle) {
    this.atomic = atomic;
    this.handle = handle;
    this.idPortMapping = idMappings(atomic.inPorts());
  }

  public static Props props(final AtomicGraph atomic, final AtomicHandle handle) {
    return Props.create(AtomicActor.class, atomic, handle);
  }

  private TLongObjectMap<InPort> idMappings(Collection<InPort> inPorts) {
    final TLongObjectMap<InPort> result = new TLongObjectHashMap<>();
    for (final InPort port : inPorts) {
      result.put(port.id(), port);
    }
    return result;
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
    atomic.onPush(idPortMapping.get(message.port()), message.payload(), handle);
  }
}
