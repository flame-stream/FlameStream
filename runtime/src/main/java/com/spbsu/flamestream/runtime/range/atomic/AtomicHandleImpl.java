package com.spbsu.flamestream.runtime.range.atomic;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.stat.Statistics;
import com.spbsu.flamestream.runtime.ack.Ack;
import com.spbsu.flamestream.runtime.range.AddressedItem;
import com.spbsu.flamestream.runtime.tick.HashMapping;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.spbsu.flamestream.runtime.tick.TickRoutes;

import java.util.function.ToIntFunction;

public final class AtomicHandleImpl implements AtomicHandle {
  private final TickInfo tickInfo;
  private final ActorContext context;
  private final LoggingAdapter LOG;
  private final TickRoutes tickRoutes;

  private final HashMapping<ActorRef> hashMapping;

  AtomicHandleImpl(TickInfo tickInfo, TickRoutes tickRoutes, ActorContext context) {
    this.tickInfo = tickInfo;
    this.tickRoutes = tickRoutes;
    this.context = context;
    LOG = Logging.getLogger(context.system(), context.self());
    this.hashMapping = HashMapping.hashMapping(tickRoutes.rangeConcierges());
  }

  @Override
  public void push(OutPort out, DataItem<?> result) {
    final InPort destination = tickInfo.graph().graph().downstreams().get(out);
    if (destination == null) throw new RoutingException("Unable to find port for " + out);

    //noinspection rawtypes
    final ToIntFunction hashFunction = destination.hashFunction();

    //noinspection unchecked
    final int hash = hashFunction.applyAsInt(result.payload());
    final AddressedItem message = new AddressedItem(result, destination);

    final ActorRef ref = hashMapping.valueFor(hash);
    ref.tell(message, context.self());

    ack(result);
  }

  @Override
  public void ack(DataItem<?> item) {
    final Ack message = new Ack(item.ack(), item.meta().globalTime());
    tickRoutes.acker().tell(message, context.self());
  }

  @Override
  public void submitStatistics(Statistics stat) {
    LOG.info("Inner statistics: {}", stat);
  }

  @Override
  public void error(String format, Object... args) {
    LOG.warning(format, args);
  }
}
