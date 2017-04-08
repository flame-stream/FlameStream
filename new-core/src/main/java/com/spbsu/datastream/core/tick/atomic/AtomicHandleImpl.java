package com.spbsu.datastream.core.tick.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.RoutingException;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.tick.AddressedMessage;
import com.spbsu.datastream.core.tick.TickContext;

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

    final HashFunction hashFunction = address.hashFunction();

    @SuppressWarnings("unchecked")
    final int hash = hashFunction.applyAsInt(result.payload());

    final AddressedMessage addressedMessage = new AddressedMessage(result, address.id(), hash);
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
  public void ack(final InPort port, final DataItem<?> dataItem) {

  }

  @Override
  public void fail(final DataItem<?> dataItem, final InPort inPort, final Exception reason) {
    throw new RuntimeException(reason);
  }

  @Override
  public Meta copyAndAppendLocal(final Meta meta) {
    return new Meta(meta, tickContext.incrementLocalTimeAndGet());
  }
}
