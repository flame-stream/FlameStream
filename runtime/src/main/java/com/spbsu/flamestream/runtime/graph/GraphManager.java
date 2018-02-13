package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.hash.Hashing;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.acker.LocalAcker;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.acker.api.UnregisterFront;
import com.spbsu.flamestream.runtime.barrier.api.AttachRear;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.materialization.ActorJoba;
import com.spbsu.flamestream.runtime.graph.materialization.GroupingJoba;
import com.spbsu.flamestream.runtime.graph.materialization.Joba;
import com.spbsu.flamestream.runtime.graph.materialization.MapJoba;
import com.spbsu.flamestream.runtime.graph.materialization.MinTimeHandler;
import com.spbsu.flamestream.runtime.graph.materialization.RouterJoba;
import com.spbsu.flamestream.runtime.graph.materialization.SinkJoba;
import com.spbsu.flamestream.runtime.graph.materialization.SourceJoba;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.collections.ListIntRangeMap;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import org.apache.commons.lang.math.IntRange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

public class GraphManager extends LoggingActor {
  private final String nodeId;
  private final Graph graph;
  private final ActorRef acker;
  private final ComputationProps computationProps;

  private final IntRangeMap<ActorRef> router = new ListIntRangeMap<>();
  private final Map<Destination, Joba> materialization = new HashMap<>();
  private final Collection<MinTimeHandler> minTimeHandlers = new ArrayList<>();
  private final Collection<Joba> allJobas = new ArrayList<>();

  private SinkJoba sinkJoba;

  private GraphManager(String nodeId,
                       Graph graph,
                       ActorRef acker,
                       ComputationProps computationProps) {
    this.nodeId = nodeId;
    this.computationProps = computationProps;
    this.graph = graph;
    this.acker = context().actorOf(LocalAcker.props(acker), "local-acker");
  }

  public static Props props(String nodeId,
                            Graph graph,
                            ActorRef acker,
                            ComputationProps layout) {
    return Props.create(GraphManager.class, nodeId, graph, acker, layout);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Map.class, managers -> {
              log().info("Finishing constructor");
              final Map<IntRange, ActorRef> routerMap = new HashMap<>();
              computationProps.ranges()
                      .forEach((key, value) -> routerMap.put(value.asRange(), (ActorRef) managers.get(key)));
              final IntRangeMap<ActorRef> intRangeMap = new ListIntRangeMap<>(routerMap);
              router.putAll(intRangeMap);

              { //materialization
                final Map<String, Joba> jobasForVertices = new HashMap<>();
                final Multimap<String, RouterJoba> routers = LinkedListMultimap.create();
                graph.vertices().forEach(vertex -> buildMaterialization(vertex, jobasForVertices, routers));
                jobasForVertices.forEach((vertexId, joba) -> {
                  if (joba instanceof GroupingJoba || joba instanceof MapJoba || joba instanceof SinkJoba) {
                    materialization.put(Destination.fromVertexId(vertexId), new ActorJoba(joba));
                  } else if (joba instanceof SourceJoba) {
                    materialization.put(Destination.fromVertexId(vertexId), joba);
                  }
                  if (joba instanceof MinTimeHandler) {
                    minTimeHandlers.add((MinTimeHandler) joba);
                  }
                  if (joba instanceof SinkJoba) {
                    sinkJoba = (SinkJoba) joba;
                  }
                  allJobas.add(joba);
                  routers.get(vertexId).forEach(routerJoba -> routerJoba.setLocalJoba(joba));
                });
              }

              unstashAll();
              getContext().become(managing());
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive managing() {
    return ReceiveBuilder.create()
            .match(DataItem.class, this::accept)
            .match(AddressedItem.class, this::inject)
            .match(MinTimeUpdate.class, this::onMinTimeUpdate)
            .match(AttachRear.class, this::attachRear)
            .match(Heartbeat.class, gt -> acker.forward(gt, context()))
            .match(UnregisterFront.class, gt -> acker.forward(gt, context()))
            .build();
  }

  private void attachRear(AttachRear attachRear) {
    sinkJoba.addRear(attachRear.rear());
  }

  private final Tracing.Tracer acceptIn = Tracing.TRACING.forEvent("accept-in", 1000, 1);
  private final Tracing.Tracer acceptOut = Tracing.TRACING.forEvent("accept-out", 1000, 1);

  private void accept(DataItem dataItem) {
    acceptIn.log(dataItem.payload(Object.class).hashCode());
    final SourceJoba joba = (SourceJoba) materialization.get(Destination.fromVertexId(graph.source().id()));
    joba.addFront(dataItem.meta().globalTime().frontId(), sender());
    joba.accept(dataItem, false);
    acceptOut.log(dataItem.payload(Object.class).hashCode());

  }

  private final Tracing.Tracer injectIn = Tracing.TRACING.forEvent("inject-in");
  private final Tracing.Tracer injectOut = Tracing.TRACING.forEvent("inject-out");

  private void inject(AddressedItem addressedItem) {
    injectIn.log(addressedItem.item().xor());
    materialization.get(addressedItem.destination()).accept(addressedItem.item(), true);
    injectOut.log(addressedItem.item().xor());
  }

  private void onMinTimeUpdate(MinTimeUpdate minTimeUpdate) {
    minTimeHandlers.forEach(minTimeHandler -> minTimeHandler.onMinTime(minTimeUpdate.minTime()));
  }

  //DFS
  private Joba buildMaterialization(Graph.Vertex vertex,
                                    Map<String, Joba> jobasForVertices,
                                    Multimap<String, RouterJoba> routers) {
    if (jobasForVertices.containsKey(vertex.id())) {
      return jobasForVertices.get(vertex.id());
    } else {
      final Stream<Joba> output = graph.adjacent(vertex)
              .map(outVertex -> {
                // TODO: 15.12.2017 add circuit breaker
                if (outVertex instanceof Grouping || ((Graph.Builder.MyGraph) graph).isShuffle(vertex, outVertex)) {
                  final HashFunction hashFunction;
                  if (outVertex instanceof Grouping) {
                    hashFunction = ((Grouping) outVertex).hash();
                  } else if (outVertex instanceof Sink) {
                    hashFunction = dataItem -> Hashing.murmur3_32().hashInt(dataItem.payload(Object.class).hashCode()).asInt();
                  } else {
                    //hashFunction = dataItem -> 2_000_000_000;
                    hashFunction = dataItem -> ThreadLocalRandom.current().nextInt();
                  }

                  final RouterJoba routerJoba = new RouterJoba(
                          router,
                          computationProps.ranges().get(nodeId),
                          hashFunction,
                          Destination.fromVertexId(outVertex.id()),
                          acker,
                          context()
                  );
                  routers.put(outVertex.id(), routerJoba);
                  return routerJoba;
                }

                if (((Graph.Builder.MyGraph) graph).isAsync(vertex, outVertex)) {
                  return new ActorJoba(buildMaterialization(outVertex, jobasForVertices, routers));
                } else {
                  return buildMaterialization(outVertex, jobasForVertices, routers);
                }
              });

      final Joba joba;
      if (vertex instanceof Sink) {
        joba = new SinkJoba(acker, context());
      } else if (vertex instanceof FlameMap) {
        joba = new MapJoba((FlameMap<?, ?>) vertex, output, acker, context());
      } else if (vertex instanceof Grouping) {
        joba = new GroupingJoba((Grouping) vertex, output, acker, context());
      } else if (vertex instanceof Source) {
        joba = new SourceJoba(computationProps.maxElementsInGraph(), output, acker, context());
      } else {
        throw new RuntimeException("Invalid vertex type");
      }
      jobasForVertices.put(vertex.id(), joba);
      return joba;
    }
  }

  public static class Destination {
    private final static Map<String, Destination> cache = new HashMap<>();
    private final String vertexId;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Destination that = (Destination) o;
      return vertexId.equals(that.vertexId);
    }

    @Override
    public int hashCode() {
      return vertexId.hashCode();
    }

    @Override
    public String toString() {
      return "Destination{" +
              "vertexId='" + vertexId + '\'' +
              '}';
    }

    private Destination(String vertexId) {
      this.vertexId = vertexId;
    }

    private static Destination fromVertexId(String vertexId) {
      return cache.compute(vertexId, (s, destination) -> {
        if (destination == null) {
          return new Destination(s);
        }
        return destination;
      });
    }
  }

  @Override
  public void postStop() {
    super.postStop();
    allJobas.forEach(j -> {
      try {
        j.onStop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }
}
