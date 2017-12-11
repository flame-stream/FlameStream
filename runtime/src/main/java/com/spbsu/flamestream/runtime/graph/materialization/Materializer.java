package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.ActorVertexJoba;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.BroadcastJoba;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.GroupingJoba;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.MapJoba;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.SourceJoba;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.VertexJoba;
import com.spbsu.flamestream.runtime.utils.collections.IntRangeMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public class Materializer implements AutoCloseable {
  private final Graph graph;
  private final IntRangeMap<Router> routers;
  private final Consumer<DataItem> barrier;
  private final Consumer<DataItem> acker;
  private final Consumer<GlobalTime> heartBeater;
  private final Map<Destination, VertexJoba> broadcasts;
  private final ActorContext context;

  private final Map<Destination, VertexJoba> jobMapping = new HashMap<>();
  private final Materialization materialization;

  public Materializer(Graph graph,
                      IntRangeMap<Router> routers,
                      Consumer<DataItem> barrier,
                      Consumer<DataItem> acker,
                      Consumer<GlobalTime> heartBeater,
                      ActorContext context) {
    this.graph = graph;
    this.routers = routers;
    this.barrier = barrier;
    this.acker = acker;
    this.heartBeater = heartBeater;
    this.context = context;
    broadcasts = new HashMap<>();

    buildJobMapping(graph.sink(), null);
    materialization = new Materialization() {
      @Override
      public void input(DataItem dataItem, ActorRef front) {
        // FIXME: 07.12.2017 PERFORMANCE
        final SourceJoba sourceJoba = (SourceJoba) jobMapping.get(new Destination(graph.source().id()));
        sourceJoba.addFront(dataItem.meta().globalTime().frontId(), front);
        sourceJoba.accept(dataItem);
      }

      @Override
      public void inject(Destination destination, DataItem dataItem) {
        final VertexJoba vertexJoba = jobMapping.get(destination);
        vertexJoba.accept(dataItem);
        if (!vertexJoba.isAsync()) {
          acker.accept(dataItem);
        }
      }

      @Override
      public void minTime(GlobalTime minTime) {
        jobMapping.values().forEach(vertexJoba -> vertexJoba.onMinTime(minTime));
      }

      @Override
      public void commit() {
        jobMapping.values().forEach(VertexJoba::onCommit);
      }
    };
  }

  public Materialization materialization() {
    return materialization;
  }

  @Override
  public void close() {
    jobMapping.values().forEach(VertexJoba::close);
  }

  //DFS
  private void buildJobMapping(Graph.Vertex currentVertex, VertexJoba outputJoba) {
    if (broadcasts.containsKey(new Destination(currentVertex.id()))) {
      ((BroadcastJoba) broadcasts.get(new Destination(currentVertex.id()))).addSink(outputJoba);
    } else {
      final VertexJoba realOutputJoba;
      if (graph.isBroadcast(currentVertex)) {
        final BroadcastJoba broadcastJoba = new BroadcastJoba();
        broadcastJoba.addSink(outputJoba);
        broadcasts.put(new Destination(currentVertex.id()), broadcastJoba);
        realOutputJoba = broadcastJoba;
      } else {
        realOutputJoba = outputJoba;
      }

      boolean isGrouping = false;
      final VertexJoba currentJoba;
      if (currentVertex instanceof Sink) {
        currentJoba = new VertexJoba() {
          @Override
          public boolean isAsync() {
            return true;
          }

          @Override
          public void accept(DataItem dataItem) {
            barrier.accept(dataItem);
          }
        };
      } else if (currentVertex instanceof FlameMap) {
        currentJoba = new ActorVertexJoba(new MapJoba((FlameMap<?, ?>) currentVertex, realOutputJoba), acker, context);
      } else if (currentVertex instanceof Grouping) {
        currentJoba = new ActorVertexJoba(new GroupingJoba((Grouping) currentVertex, realOutputJoba), acker, context);
        isGrouping = true;
      } else if (currentVertex instanceof Source) {
        // TODO: 04.12.2017 choose number depends on statistics
        currentJoba = new SourceJoba(10, context, heartBeater, realOutputJoba);
      } else {
        throw new RuntimeException("Invalid vertex type");
      }
      jobMapping.put(new Destination(currentVertex.id()), currentJoba);

      final VertexJoba currentAsNext;
      if (isGrouping) {
        currentAsNext = dataItem -> {
          final int hash = ((Grouping) currentVertex).hash().applyAsInt(dataItem);
          routers.get(hash).route(dataItem, new Destination(currentVertex.id()));
          acker.accept(dataItem);
        };
      } else if (currentJoba.isAsync()) {
        currentAsNext = dataItem -> {
          currentJoba.accept(dataItem);
          acker.accept(dataItem);
        };
      } else {
        currentAsNext = currentJoba;
      }
      graph.inputs(currentVertex).forEach(vertex -> buildJobMapping(vertex, currentAsNext));
    }
  }

  public static class Destination {
    private final String vertexId;

    Destination(String vertexId) {
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
