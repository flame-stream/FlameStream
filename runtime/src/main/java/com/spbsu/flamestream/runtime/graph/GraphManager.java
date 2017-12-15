package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.config.ComputationLayout;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.materialization.GroupingJoba;
import com.spbsu.flamestream.runtime.graph.materialization.Joba;
import com.spbsu.flamestream.runtime.graph.materialization.MapJoba;
import com.spbsu.flamestream.runtime.graph.materialization.MinTimeHandler;
import com.spbsu.flamestream.runtime.graph.materialization.RouterJoba;
import com.spbsu.flamestream.runtime.graph.materialization.SinkJoba;
import com.spbsu.flamestream.runtime.graph.materialization.SourceJoba;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphManager extends LoggingActor {
  private final String nodeId;
  private final Graph graph;
  private final ActorRef acker;
  private final ComputationLayout layout;
  private final BiConsumer<DataItem, ActorRef> barrier;

  private final Map<String, ActorRef> managerRefs = new HashMap<>();
  private final Map<Destination, Joba> materialization = new HashMap<>();
  private final Collection<MinTimeHandler> minTimeHandlers = new ArrayList<>();

  private GraphManager(String nodeId,
                       Graph graph,
                       ActorRef acker,
                       ComputationLayout layout,
                       BiConsumer<DataItem, ActorRef> barrier) {
    this.nodeId = nodeId;
    this.layout = layout;
    this.graph = graph;
    this.acker = acker;
    this.barrier = barrier;
  }

  public static Props props(String nodeId,
                            Graph graph,
                            ActorRef acker,
                            ComputationLayout layout,
                            BiConsumer<DataItem, ActorRef> barrier) {
    return Props.create(GraphManager.class, nodeId, graph, acker, layout, barrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Map.class, managers -> {
              log().info("Finishing constructor");
              //noinspection unchecked
              managerRefs.putAll(managers);

              final Map<String, Joba> allJobas = new HashMap<>();
              graph.vertices().forEach(vertex -> buildMaterialization(vertex, allJobas));
              minTimeHandlers.addAll(
                      allJobas.values()
                              .stream()
                              .filter(joba -> joba instanceof MinTimeHandler)
                              .map(joba -> (MinTimeHandler) joba)
                              .collect(Collectors.toList())
              );

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
            .build();
  }

  private void accept(DataItem dataItem) {
    materialization.get(Destination.fromVertexId(graph.source().id())).accept(dataItem, false);
  }

  private void inject(AddressedItem addressedItem) {
    materialization.get(addressedItem.destination()).accept(addressedItem.item(), true);
  }

  private void onMinTimeUpdate(MinTimeUpdate minTimeUpdate) {
    minTimeHandlers.forEach(minTimeHandler -> minTimeHandler.onMinTime(minTimeUpdate.minTime()));
  }

  //DFS
  private Joba buildMaterialization(Graph.Vertex vertex, Map<String, Joba> allJobas) {
    if (allJobas.containsKey(vertex.id())) {
      return allJobas.get(vertex.id());
    } else {
      final Stream<Joba> output = graph.adjacent(vertex)
              .map(outVertex -> {
                // TODO: 15.12.2017 add circuit breaker
                if (outVertex instanceof Grouping) {
                  return new RouterJoba(
                          layout,
                          managerRefs,
                          ((Grouping) outVertex).hash(),
                          Destination.fromVertexId(outVertex.id()),
                          context());
                }
                return buildMaterialization(outVertex, allJobas);
              });

      final Joba joba;
      if (vertex instanceof Sink) {
        joba = new SinkJoba(barrier, acker, context());
      } else if (vertex instanceof FlameMap) {
        joba = new MapJoba((FlameMap<?, ?>) vertex, output, acker, context());
      } else if (vertex instanceof Grouping) {
        joba = new GroupingJoba((Grouping) vertex, output, acker, context());
      } else if (vertex instanceof Source) {
        //this number will be computed using some intelligent method someday :)
        joba = new SourceJoba(10, output, acker, context());
      } else {
        throw new RuntimeException("Invalid vertex type");
      }

      if (vertex instanceof Source || vertex instanceof Grouping) {
        materialization.put(Destination.fromVertexId(vertex.id()), joba);
      }
      allJobas.put(vertex.id(), joba);
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
