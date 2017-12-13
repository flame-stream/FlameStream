package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.Joba;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.SourceJoba;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public class Materializer {
  private final Materialization materialization;

  public Materializer(Graph graph,
                      IntRangeMap<Router> routers,
                      Consumer<DataItem> barrier,
                      Consumer<DataItem> acker,
                      Consumer<GlobalTime> heartBeater,
                      ActorContext context) {
    final Map<String, Joba.CachedBuilder> builderMap = new HashMap<>();
    createBuilders(graph, graph.sink(), null, builderMap);

    final Map<String, Joba> jobaMap = builderMap.entrySet()
            .stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().build(acker, heartBeater, barrier, routers, context))
            );
    materialization = new Materialization() {
      @Override
      public void input(DataItem dataItem, ActorRef front) {
        final SourceJoba sourceJoba = (SourceJoba) jobaMap.get(graph.source().id());
        sourceJoba.addFront(dataItem.meta().globalTime().frontId(), front);
        sourceJoba.accept(dataItem, false);
      }

      @Override
      public void inject(Destination destination, DataItem dataItem) {
        final Joba joba = jobaMap.get(destination.vertexId);
        joba.accept(dataItem, true);
      }

      @Override
      public void minTime(GlobalTime minTime) {
      }

      @Override
      public void commit() {
      }
    };
  }

  public Materialization materialization() {
    return materialization;
  }

  //DFS
  private void createBuilders(Graph graph,
                              Graph.Vertex currentVertex,
                              Joba.CachedBuilder output,
                              Map<String, Joba.CachedBuilder> builderMap) {
    if (builderMap.containsKey(currentVertex.id())) {
      builderMap.get(currentVertex.id()).addOutput(output);
    } else {
      final Joba.CachedBuilder currentJoba = new Joba.CachedBuilder(currentVertex).addOutput(output);
      builderMap.put(currentVertex.id(), currentJoba);
      graph.inputs(currentVertex).forEach(vertex -> createBuilders(graph, vertex, currentJoba, builderMap));
    }
  }

  public static class Destination {
    private final String vertexId;

    public Destination(String vertexId) {
      this.vertexId = vertexId;
    }

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
  }
}
