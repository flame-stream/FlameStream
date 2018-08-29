package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.master.acker.api.Ack;
import com.spbsu.flamestream.runtime.master.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.master.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.master.acker.api.commit.Prepare;
import com.spbsu.flamestream.runtime.master.acker.api.registry.UnregisterFront;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.api.NewRear;
import com.spbsu.flamestream.runtime.graph.api.ComponentPrepared;
import com.spbsu.flamestream.runtime.graph.state.GroupGroupingState;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.HashUnitMap;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
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

  private final Tracing.Tracer shuffleSendTracer = Tracing.TRACING.forEvent("shuffle-send");
  private final Tracing.Tracer groupingSendTracer = Tracing.TRACING.forEvent("fm-send");
  private final Tracing.Tracer acceptInTracer = Tracing.TRACING.forEvent("accept-in", 1000, 1);
  private final Tracing.Tracer acceptOutTracer = Tracing.TRACING.forEvent("accept-in", 1000, 1);
  private final Tracing.Tracer injectInTracer = Tracing.TRACING.forEvent("inject-in");
  private final Tracing.Tracer injectOutTracer = Tracing.TRACING.forEvent("inject-out");

  @Nullable
  private SourceJoba sourceJoba;
  @Nullable
  private GraphManager.Destination sourceDestination;

  @Nullable
  private SinkJoba sinkJoba;

  private Component(Set<Graph.Vertex> componentVertices,
                    Graph graph,
                    HashUnitMap<ActorRef> routes,
                    ActorRef localManager,
                    ActorRef acker,
                    ComputationProps props,
                    Map<String, GroupGroupingState> stateByVertex) {
    this.acker = acker;

    {
      jobas = new HashMap<>();
      for (Graph.Vertex vertex : componentVertices) {
        final Joba joba;
        if (vertex instanceof Sink) {
          joba = new SinkJoba(context());
        } else if (vertex instanceof FlameMap) {
          joba = new MapJoba((FlameMap<?, ?>) vertex);
        } else if (vertex instanceof Grouping) {
          final Grouping grouping = (Grouping) vertex;
          final Collection<HashUnit> values = props.hashGroups()
                  .values()
                  .stream()
                  .flatMap(g -> g.units().stream())
                  .collect(Collectors.toSet());
          stateByVertex.putIfAbsent(vertex.id(), new GroupGroupingState(values));
          joba = new GroupingJoba(grouping, stateByVertex.get(vertex.id()));
        } else if (vertex instanceof Source) {
          joba = new SourceJoba(props.maxElementsInGraph(), context());
        } else {
          throw new RuntimeException("Invalid vertex type");
        }
        jobas.put(GraphManager.Destination.fromVertexId(vertex.id()), joba);
        if (joba instanceof SourceJoba) {
          sourceJoba = (SourceJoba) joba;
          sourceDestination = GraphManager.Destination.fromVertexId(vertex.id());
        } else if (joba instanceof SinkJoba) {
          sinkJoba = (SinkJoba) joba;
        }
      }
    }

    for (Graph.Vertex from : componentVertices) {
      final GraphManager.Destination fromDest = GraphManager.Destination.fromVertexId(from.id());

      final Set<Consumer<DataItem>> sinks = graph.adjacent(from)
              .map(to -> {
                final GraphManager.Destination toDest = GraphManager.Destination.fromVertexId(to.id());

                final Consumer<DataItem> sink;
                if (componentVertices.contains(to)) {
                  sink = item -> localCall(item, toDest);
                } else if (graph.isShuffle(from, to)) {
                  sink = item -> {
                    shuffleSendTracer.log(item.xor());
                    acker.tell(new Ack(item.meta().globalTime(), item.xor()), self());
                    routes.get(ThreadLocalRandom.current().nextInt())
                            .tell(new AddressedItem(item, toDest), self());
                  };
                } else if (to instanceof Grouping) {
                  sink = item -> {
                    groupingSendTracer.log(item.xor());
                    acker.tell(new Ack(item.meta().globalTime(), item.xor()), self());
                    routes.get(((Grouping) to).hash().applyAsInt(item))
                            .tell(new AddressedItem(item, toDest), self());
                  };
                } else {
                  sink = item -> {
                    acker.tell(new Ack(item.meta().globalTime(), item.xor()), self());
                    localManager.tell(new AddressedItem(item, toDest), self());
                  };
                }
                return sink;
              })
              .collect(Collectors.toSet());

      if (sinks.size() == 1) {
        downstreams.put(fromDest, sinks.stream().findAny().get());
      } else if (sinks.size() > 1) {
        final Consumer<DataItem> broadcast = item -> {
          final int[] childId = {0};
          for (Consumer<DataItem> sink : sinks) {
            final Meta newMeta = new Meta(item.meta(), 0, childId[0]);
            final DataItem newItem = new BroadcastDataItem(item, newMeta);
            sink.accept(newItem);
            childId[0]++;
          }
        };
        downstreams.put(fromDest, broadcast);
      } else {
        downstreams.put(fromDest, item -> context().system().deadLetters().tell(item, self()));
      }
    }
  }

  public static Props props(Set<Graph.Vertex> componentVertices,
                            Graph graph,
                            HashUnitMap<ActorRef> localManager,
                            ActorRef routes,
                            ActorRef acker,
                            ComputationProps props,
                            Map<String, GroupGroupingState> stateByVertex) {
    return Props.create(Component.class, componentVertices, graph, localManager, routes, acker, props, stateByVertex)
            .withDispatcher("processing-dispatcher");
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AddressedItem.class, this::inject)
            .match(DataItem.class, this::accept)
            .match(MinTimeUpdate.class, this::onMinTime)
            .match(NewRear.class, this::onNewRear)
            .match(Heartbeat.class, h -> acker.forward(h, context()))
            .match(UnregisterFront.class, u -> acker.forward(u, context()))
            .match(Prepare.class, this::onPrepare)
            .build();
  }

  private void onPrepare(Prepare prepare) {
    jobas.values().forEach(joba -> joba.onPrepareCommit(prepare.globalTime()));
    sender().tell(new ComponentPrepared(), self());
  }

  private void inject(AddressedItem addressedItem) {
    final DataItem item = addressedItem.item();
    injectInTracer.log(item.xor());
    localCall(item, addressedItem.destination());
    acker.tell(new Ack(item.meta().globalTime(), item.xor()), self());
    injectOutTracer.log(item.xor());
  }

  private void localCall(DataItem item, GraphManager.Destination destination) {
    jobas.get(destination).accept(item, downstreams.get(destination));
  }

  private void onMinTime(MinTimeUpdate minTimeUpdate) {
    jobas.values().forEach(j -> j.onMinTime(minTimeUpdate.minTime()));
  }

  private void accept(DataItem item) {
    if (sourceJoba != null) {
      acceptInTracer.log(item.xor());
      sourceJoba.addFront(item.meta().globalTime().frontId(), sender());
      sourceJoba.accept(item, downstreams.get(sourceDestination));
      acceptOutTracer.log(item.xor());
    } else {
      throw new IllegalStateException("Source doesn't belong to this component");
    }
  }

  private void onNewRear(NewRear attachRear) {
    if (sinkJoba != null) {
      sinkJoba.attachRear(attachRear.rear());
    } else {
      throw new IllegalStateException("Sink doesn't belong to this component");
    }
  }

  private static class BroadcastDataItem implements DataItem {
    private final DataItem inner;
    private final Meta newMeta;
    private final long xor;

    BroadcastDataItem(DataItem inner, Meta newMeta) {
      this.inner = inner;
      this.newMeta = newMeta;
      this.xor = ThreadLocalRandom.current().nextLong();
    }

    @Override
    public Meta meta() {
      return newMeta;
    }

    @Override
    public <T> T payload(Class<T> expectedClass) {
      return inner.payload(expectedClass);
    }

    @Override
    public long xor() {
      return xor;
    }
  }
}
