package com.spbsu.datastream.core.range.atomic;

import akka.actor.ActorContext;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.ack.Ack;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.range.AddressedItem;
import com.spbsu.datastream.core.stat.Statistics;
import com.spbsu.datastream.core.tick.HashMapping;
import com.spbsu.datastream.core.tick.TickRoutes;
import com.spbsu.datastream.core.tick.TickInfo;

import java.util.function.ToIntFunction;

public final class AtomicHandleImpl implements AtomicHandle {
  private final TickInfo tickInfo;
  private final ActorContext context;
  private final LoggingAdapter LOG;
  private final TickRoutes tickRoutes;

  private final HashMapping<ActorRef> hashMapping;

  public AtomicHandleImpl(TickInfo tickInfo, TickRoutes tickRoutes, ActorContext context) {
    this.tickInfo = tickInfo;
    this.tickRoutes = tickRoutes;
    this.context = context;
    LOG = Logging.getLogger(context.system(), context.self());
    this.hashMapping = HashMapping.hashMapping(tickRoutes.rangeConcierges());
  }

  @Override
  public ActorSelection actorSelection(ActorPath path) {
    return context.actorSelection(path);
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
    final int id = tickInfo.ackerLocation();

    final Ack message = new Ack(item.ack(), item.meta().globalTime());
    tickRoutes.acker().tell(message, context.self());
  }

  @Override
  public void submitStatistics(Statistics stat) {
    LOG.info("Inner statistics: {}", stat);
  }

  @Override
  public TickInfo tickInfo() {
    return tickInfo;
  }

  @Override
  public void error(String format, Object... args) {
    LOG.warning(format, args);
  }
}
