package com.spbsu.flamestream.runtime.node.materializer.graph.atomic;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.ComposedGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.OutPort;
import com.spbsu.flamestream.core.utils.Statistics;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.node.materializer.GraphRoutes;
import com.spbsu.flamestream.runtime.node.materializer.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.utils.HashMapping;
import com.spbsu.flamestream.runtime.utils.ListHashMapping;

import java.util.function.ToIntFunction;

public class AtomicHandleImpl implements AtomicHandle {
  protected final ActorContext context;
  protected final GraphRoutes tickRoutes;

  private final LoggingAdapter log;
  private final HashMapping<ActorRef> hashMapping;
  private final ComposedGraph<AtomicGraph> graph;

  public AtomicHandleImpl(ComposedGraph<AtomicGraph> graph, GraphRoutes graphRoutes, ActorContext context) {
    this.tickRoutes = graphRoutes;
    this.context = context;
    this.graph = graph;
    log = Logging.getLogger(context.system(), context.self());
    this.hashMapping = new ListHashMapping<>(graphRoutes.graphs());
  }

  @Override
  public void push(OutPort out, DataItem<?> result) {
    final InPort destination = graph.downstreams().get(out);

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
