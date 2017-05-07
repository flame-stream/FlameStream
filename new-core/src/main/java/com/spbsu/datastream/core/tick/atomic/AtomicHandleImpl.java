package com.spbsu.datastream.core.tick.atomic;

import akka.actor.ActorContext;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.RoutingException;
import com.spbsu.datastream.core.ack.Ack;
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
  private final ActorContext context;

  public AtomicHandleImpl(final TickContext tickContext, final ActorContext context) {
    this.tickContext = tickContext;
    this.context = context;
  }

  @Override
  public ActorSelection actorSelection(final ActorPath path) {
    return this.context.actorSelection(path);
  }

  @Override
  public void push(final OutPort out, final DataItem<?> result) {
    final Optional<InPort> destination = Optional.ofNullable(this.tickContext.tickInfo().graph().graph().downstreams().get(out));
    final InPort address = destination.orElseThrow(() -> new RoutingException("Unable to find port for " + out));

    @SuppressWarnings("rawtypes") final HashFunction hashFunction = address.hashFunction();

    @SuppressWarnings("unchecked") final int hash = hashFunction.applyAsInt(result.payload());

    final AddressedMessage<?> addressedMessage = new AddressedMessage<>(new PortBindDataItem(result, address), hash, this.tickContext.tickInfo().startTs());
    this.ack(result);
    this.tickContext.rootRouter().tell(addressedMessage, ActorRef.noSender());
  }

  @Override
  public void ack(final DataItem<?> item) {
    final int hash = this.tickContext.tickInfo().ackerRange().from();

    final AddressedMessage<?> addressedMessage = new AddressedMessage<>(new Ack(item.ack(), item.meta().globalTime()), hash, this.tickContext.tickInfo().startTs());
    this.tickContext.rootRouter().tell(addressedMessage, ActorRef.noSender());
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
