package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.hash.Hashing;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.acker.api.UnregisterFront;
import com.spbsu.flamestream.runtime.barrier.api.AttachRear;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Component extends LoggingActor {
  private final ActorRef acker;
  private final Map<GraphManager.Destination, Joba> jobas;
  private final Map<GraphManager.Destination, Consumer<DataItem>> downstreams = new HashMap<>();

  private final Tracing.Tracer fmSend = Tracing.TRACING.forEvent("fm-send");
  private final Tracing.Tracer shuffleSend = Tracing.TRACING.forEvent("shuffle-send");
  private final Tracing.Tracer accept = Tracing.TRACING.forEvent("accept-in", 1000, 1);
  private final Tracing.Tracer injectIn = Tracing.TRACING.forEvent("inject-in");
  private final Tracing.Tracer injectOut = Tracing.TRACING.forEvent("inject-out");

  @Nullable
  private SourceJoba sourceJoba;
  @Nullable
  private GraphManager.Destination sourceDestanation;

  @Nullable
  private SinkJoba sinkJoba;

  private Component(Set<Graph.Vertex> componentVertices,
                    Graph graph,
                    IntRangeMap<ActorRef> routes,
                    ActorRef localManager,
                    ActorRef acker,
                    ComputationProps props) {
    this.acker = acker;
    jobas = new HashMap<>();

    for (Graph.Vertex vertex : componentVertices) {
      final Joba joba = jobaFor(vertex, props);
      jobas.put(GraphManager.Destination.fromVertexId(vertex.id()), joba);
      if (joba instanceof SourceJoba) {
        sourceJoba = (SourceJoba) joba;
        sourceDestanation = GraphManager.Destination.fromVertexId(vertex.id());
      } else if (joba instanceof SinkJoba) {
        sinkJoba = (SinkJoba) joba;
      }
    }

    for (Graph.Vertex from : componentVertices) {
      final Set<Consumer<DataItem>> sinks = graph.adjacent(from)
              .map(to -> {
                final GraphManager.Destination destination = GraphManager.Destination.fromVertexId(to.id());

                final Consumer<DataItem> sink;
                if (componentVertices.contains(to)) {
                  sink = item -> localCall(item, destination);
                } else if (graph.isShuffle(from, to)) {
                  sink = item -> {
                    shuffleSend.log(item.xor());
                    acker.tell(new Ack(item.meta().globalTime(), item.xor()), self());
                    routes.get(ThreadLocalRandom.current().nextInt())
                            .tell(new AddressedItem(item, destination), self());
                  };
                } else if (to instanceof Grouping) {
                  sink = item -> {
                    fmSend.log(item.xor());
                    acker.tell(new Ack(item.meta().globalTime(), item.xor()), self());
                    routes.get(((Grouping) to).hash().applyAsInt(item))
                            .tell(new AddressedItem(item, destination), self());
                  };
                } else {
                  sink = item -> {
                    acker.tell(new Ack(item.meta().globalTime(), item.xor()), self());
                    localManager.tell(new AddressedItem(item, destination), self());
                  };
                }
                return sink;
              })
              .collect(Collectors.toSet());

      final GraphManager.Destination key = GraphManager.Destination.fromVertexId(from.id());

      if (sinks.size() == 1) {
        downstreams.put(key, sinks.stream().findAny().get());
      } else if (sinks.size() > 1) {
        final Consumer<DataItem> broadcast = item -> {
          final int[] childId = {0};
          for (Consumer<DataItem> sink : sinks) {
            final Meta newMeta = new Meta(item.meta(), 0, childId[0]);
            final DataItem newItem = new BroadcastDataItem(item, newMeta);
            sink.accept(new BroadcastDataItem(newItem, newMeta));
            childId[0]++;
          }
        };
        downstreams.put(key, broadcast);
      } else {
        downstreams.put(key, item -> context().system().deadLetters().tell(item, self()));
      }
    }
  }

  private Joba jobaFor(Graph.Vertex vertex, ComputationProps props) {
    final Joba joba;
    if (vertex instanceof Sink) {
      joba = new SinkJoba(context());
    } else if (vertex instanceof FlameMap) {
      joba = new MapJoba((FlameMap<?, ?>) vertex);
    } else if (vertex instanceof Grouping) {
      joba = new GroupingJoba((Grouping) vertex);
    } else if (vertex instanceof Source) {
      joba = new SourceJoba(props.maxElementsInGraph(), context());
    } else {
      throw new RuntimeException("Invalid vertex type");
    }

    return joba;
  }

  public static Props props(Set<Graph.Vertex> componentVertices,
                            Graph graph,
                            IntRangeMap<ActorRef> localManager,
                            ActorRef routes,
                            ActorRef acker,
                            ComputationProps props) {
    return Props.create(Component.class, componentVertices, graph, localManager, routes, acker, props);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AddressedItem.class, this::inject)
            .match(DataItem.class, this::accept)
            .match(MinTimeUpdate.class, this::onMinTime)
            .match(AttachRear.class, this::attachRear)
            .match(Heartbeat.class, h -> acker.forward(h, context()))
            .match(UnregisterFront.class, u -> acker.forward(u, context()))
            .build();
  }

  private void inject(AddressedItem addressedItem) {
    final DataItem item = addressedItem.item();
    injectIn.log(item.xor());
    localCall(item, addressedItem.destination());
    acker.tell(new Ack(item.meta().globalTime(), item.xor()), self());
    injectOut.log(item.xor());
  }

  private void localCall(DataItem item, GraphManager.Destination destination) {
    jobas.get(destination).accept(item, downstreams.get(destination));
  }

  private void onMinTime(MinTimeUpdate minTimeUpdate) {
    jobas.values().forEach(j -> j.onMinTime(minTimeUpdate.minTime()));
  }

  private void accept(DataItem item) {
    if (sourceJoba != null) {
      accept.log(item.xor());
      sourceJoba.addFront(item.meta().globalTime().frontId(), sender());
      sourceJoba.accept(item, downstreams.get(sourceDestanation));
    } else {
      throw new IllegalStateException("Source doesn't belong to this component");
    }
  }

  private void attachRear(AttachRear attachRear) {
    if (sinkJoba != null) {
      sinkJoba.attachRear(attachRear.rear());
    } else {
      throw new IllegalStateException("Sink doesn't belong to this component");
    }
  }
}
