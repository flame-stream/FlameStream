package com.spbsu.datastream.core.tick.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.RoutingException;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.graph.ops.GroupingState;
import com.spbsu.datastream.core.range.AddressedMessage;
import com.spbsu.datastream.core.tick.PortBindDataItem;
import com.spbsu.datastream.core.tick.TickContext;

import java.util.Optional;

public final class AtomicHandleImpl implements AtomicHandle {
  private final TickContext tickContext;

  public AtomicHandleImpl(final TickContext tickContext) {
    this.tickContext = tickContext;
  }

  @Override
  public void push(final OutPort out, final DataItem<?> result) {
    final Optional<InPort> destination = Optional.ofNullable(this.tickContext.graph().graph().downstreams().get(out));
    final InPort address = destination.orElseThrow(() -> new RoutingException("Unable to find port for " + out));

    @SuppressWarnings("rawtypes") final HashFunction hashFunction = address.hashFunction();

    @SuppressWarnings("unchecked")
    final int hash = hashFunction.applyAsInt(result.payload());

    final AddressedMessage<?> addressedMessage = new AddressedMessage<>(new PortBindDataItem(result, address), hash, this.tickContext.tick());
    this.tickContext.rootRouter().tell(addressedMessage, null);
  }

  @Override
  public GroupingState<?> loadGroupingState() {
    //TODO: 4/11/17 load from LevelDB
    return null;
  }

  @Override
  public void saveGroupingState(final GroupingState<?> storage) {
    //TODO: 4/11/17 save to LevelDB
  }

  @Override
  public HashRange localRange() {
    return this.tickContext.localRange();
  }
}
