package com.spbsu.flamestream.runtime.node.tick.range.atomic;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.flamestream.common.Statistics;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.node.tick.range.api.AddressedItem;
import com.spbsu.flamestream.runtime.node.tick.HashMapping;
import com.spbsu.flamestream.runtime.node.tick.api.TickInfo;
import com.spbsu.flamestream.runtime.node.tick.api.TickRoutes;

import java.util.function.ToIntFunction;

public class AtomicHandleImpl implements AtomicHandle {
  protected final ActorContext context;
  protected final TickRoutes tickRoutes;

  private final LoggingAdapter log;
  private final TickInfo tickInfo;
  private final HashMapping<ActorRef> hashMapping;

  public AtomicHandleImpl(TickInfo tickInfo, TickRoutes tickRoutes, ActorContext context) {
    this.tickInfo = tickInfo;
    this.tickRoutes = tickRoutes;
    this.context = context;
    log = Logging.getLogger(context.system(), context.self());
    this.hashMapping = HashMapping.hashMapping(tickRoutes.rangeConcierges());
  }

  @Override
  public void push(OutPort out, DataItem<?> result) {
    final InPort destination = tickInfo.graph().downstreams().get(out);

    //noinspection rawtypes
    final ToIntFunction hashFunction = destination.hashFunction();

    //noinspection unchecked
    final int hash = hashFunction.applyAsInt(result.payload());
    final AddressedItem message = new AddressedItem(result, destination);

    final ActorRef ref = hashMapping.valueFor(hash);
    ref.tell(message, context.self());
  }

  @Override
  public void ack(long xor, GlobalTime globalTime) {
    final Ack message = new Ack(globalTime, xor);
    tickRoutes.acker().tell(message, context.self());
  }

  @Override
  public void submitStatistics(Statistics stat) {
    log.info("Inner statistics: {}", stat);
  }

  @Override
  public void error(String format, Object... args) {
    log.warning(format, args);
  }

  public ActorContext backdoor() {
    return context;
  }
}
