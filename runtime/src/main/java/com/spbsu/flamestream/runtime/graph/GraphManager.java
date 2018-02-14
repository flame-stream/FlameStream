package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.acker.LocalAcker;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.acker.api.UnregisterFront;
import com.spbsu.flamestream.runtime.barrier.api.AttachRear;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;
import com.spbsu.flamestream.runtime.utils.collections.ListIntRangeMap;
import org.apache.commons.lang.math.IntRange;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class GraphManager extends LoggingActor {
  private final Graph graph;
  private final ActorRef acker;
  private final ComputationProps computationProps;

  private ActorRef sourceComponent;
  private ActorRef sinkComponent;

  private final IntRangeMap<ActorRef> routes = new ListIntRangeMap<>();
  private final Map<Destination, ActorRef> verticesComponents = new HashMap<>();
  private final Set<ActorRef> components = new HashSet<>();

  private GraphManager(Graph graph,
                       ActorRef acker,
                       ComputationProps computationProps) {
    this.computationProps = computationProps;
    this.graph = graph;
    this.acker = context().actorOf(LocalAcker.props(acker), "local-acker");
  }

  public static Props props(Graph graph,
                            ActorRef acker,
                            ComputationProps layout) {
    return Props.create(GraphManager.class, graph, acker, layout);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Map.class, managers -> {
              log().info("Finishing constructor");
              final Map<IntRange, ActorRef> routerMap = new HashMap<>();
              computationProps.ranges()
                      .forEach((key, value) -> routerMap.put(value.asRange(), (ActorRef) managers.get(key)));
              routes.putAll(new ListIntRangeMap<>(routerMap));

              graph.components().forEach(c -> {
                final Set<Graph.Vertex> vertexSet = c.collect(Collectors.toSet());

                final ActorRef component = context().actorOf(Component.props(
                        vertexSet,
                        graph,
                        routes,
                        self(),
                        acker,
                        computationProps
                ));

                vertexSet.stream()
                        .map(v -> Destination.fromVertexId(v.id()))
                        .forEach(dest -> verticesComponents.put(dest, component));

                components.add(component);

                vertexSet.stream()
                        .filter(v -> v instanceof Source)
                        .findAny()
                        .ifPresent(v -> sourceComponent = component);

                vertexSet.stream()
                        .filter(v -> v instanceof Sink)
                        .findAny()
                        .ifPresent(v -> sinkComponent = component);
              });

              unstashAll();
              getContext().become(managing());
            })
            .matchAny(m -> stash())
            .build();
  }

  private Receive managing() {
    return ReceiveBuilder.create()
            .match(DataItem.class, dataItem -> sourceComponent.forward(dataItem, context()))
            .match(
                    AddressedItem.class,
                    addressedItem -> verticesComponents.get(addressedItem.destination())
                            .forward(addressedItem, context())
            )
            .match(
                    MinTimeUpdate.class,
                    minTimeUpdate -> components.forEach(c -> c.forward(minTimeUpdate, context()))
            )
            .match(AttachRear.class, attachRear -> sinkComponent.forward(attachRear, context()))
            .match(Heartbeat.class, gt -> sourceComponent.forward(gt, context()))
            .match(UnregisterFront.class, u -> sourceComponent.forward(u, context()))
            .build();
  }

  public static class Destination {
    private static final Map<String, Destination> cache = new HashMap<>();
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

    static Destination fromVertexId(String vertexId) {
      return cache.compute(vertexId, (s, destination) -> {
        if (destination == null) {
          return new Destination(s);
        }
        return destination;
      });
    }
  }
}
