package com.spbsu.datastream.core.materializer.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.materializer.AddressedMessage;
import com.spbsu.datastream.core.materializer.RoutingException;
import com.spbsu.datastream.core.materializer.TickContext;

import java.util.Optional;

public class AtomicHandleImpl implements AtomicHandle {
  private final TickContext tickContext;

  public AtomicHandleImpl(final TickContext tickContext) {
    this.tickContext = tickContext;
  }

  @Override
  public void push(final OutPort out, final DataItem<?> result) {
    final Optional<InPort> destination = Optional.ofNullable(tickContext.graph().downstreams().get(out));
    final InPort address = destination.orElseThrow(() -> new RoutingException("Unable to find port for " + out));

    final HashFunction hashFunction =
            tickContext.graph().portHashing().getOrDefault(address, HashFunction.OBJECT_HASH);

    @SuppressWarnings("unchecked")
    final int hash = hashFunction.applyAsInt(result.payload());

    final AddressedMessage addressedMessage = new AddressedMessage(result, address, hash, false);
    tickContext.rootRouter().tell(addressedMessage, null);
  }

  @Override
  public void deploy(final TheGraph graph) {

  }

  @Override
  public void panic(final Exception e) {
    throw new RuntimeException(e);
  }

  @Override
  public void ack(final DataItem<?> dataItem) {

  }

  @Override
  public void fail(final DataItem<?> dataItem, final Exception reason) {
    throw new RuntimeException(reason);
  }
}
