package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.acker.api.UnregisterFront;
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
import org.apache.commons.lang.math.IntRange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class GraphManager extends LoggingActor {
  private final String nodeId;
  private final Graph graph;
  private final ActorRef acker;
  private final ComputationProps computationProps;
  private final Map<String, ActorRef> barriers;

  private final IntRangeMap<ActorRef> router = new ListIntRangeMap<>();
  private final Map<Destination, Joba> materialization = new HashMap<>();
  private final Collection<MinTimeHandler> minTimeHandlers = new ArrayList<>();

  private GraphManager(String nodeId,
                       Graph graph,
                       ActorRef acker,
                       ComputationProps computationProps,
                       Map<String, ActorRef> barriers) {
    this.nodeId = nodeId;
    this.computationProps = computationProps;
    this.graph = graph;
    this.acker = acker;
    this.barriers = barriers;
  }

  public static Props props(String nodeId,
                            Graph graph,
                            ActorRef acker,
                            ComputationProps layout,
                            Map<String, ActorRef> barriers) {
    return Props.create(GraphManager.class, nodeId, graph, acker, layout, barriers);
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
                  if (joba instanceof GroupingJoba || joba instanceof SourceJoba) {
                    materialization.put(Destination.fromVertexId(vertexId), joba);
                  }
                  if (joba instanceof MinTimeHandler) {
                    minTimeHandlers.add((MinTimeHandler) joba);
                  }
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
            .match(Heartbeat.class, gt -> acker.forward(gt, context()))
            .match(UnregisterFront.class, gt -> acker.forward(gt, context()))
            .build();
  }

  private void accept(DataItem dataItem) {
    final SourceJoba joba = (SourceJoba) materialization.get(Destination.fromVertexId(graph.source().id()));
    joba.addFront(dataItem.meta().globalTime().frontId(), sender());
    joba.accept(dataItem, false);
  }

  private void inject(AddressedItem addressedItem) {
    materialization.get(addressedItem.destination()).accept(addressedItem.item(), true);
  }

  private void onMinTimeUpdate(MinTimeUpdate minTimeUpdate) {
    minTimeHandlers.forEach(minTimeHandler -> minTimeHandler.onMinTime(minTimeUpdate.minTime()));
  }

  //DFS
  private Joba buildMaterialization(Graph.Vertex vertex, Map<String, Joba> jobasForVertices, Multimap<String, RouterJoba> routers) {
    if (jobasForVertices.containsKey(vertex.id())) {
      return jobasForVertices.get(vertex.id());
    } else {
      final Stream<Joba> output = graph.adjacent(vertex)
              .map(outVertex -> {
                // TODO: 15.12.2017 add circuit breaker
                if (outVertex instanceof Grouping) {
                  final RouterJoba routerJoba = new RouterJoba(
                          router,
                          computationProps.ranges().get(nodeId),
                          ((Grouping) outVertex).hash(),
                          Destination.fromVertexId(outVertex.id()),
                          acker,
                          context());
                  if (!(vertex instanceof FlameMap) || !((FlameMap) vertex).clazz().getSimpleName().equals("WikipediaPage")) {
                    routers.put(outVertex.id(), routerJoba);
                  }
                  return routerJoba;
                }
                return buildMaterialization(outVertex, jobasForVertices, routers);
              });

      final Joba joba;
      if (vertex instanceof Sink) {
        joba = new SinkJoba(barriers, acker, context());
      } else if (vertex instanceof FlameMap) {
        if (!((FlameMap) vertex).clazz().getSimpleName().equals("WikipediaPage")) {
          joba = new MapJoba((FlameMap<?, ?>) vertex, output, acker, context());
        } else {
          joba = new ActorJoba(new MapJoba((FlameMap<?, ?>) vertex, output, acker, context()));
        }
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
}
