package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public class GraphMaterializer implements AutoCloseable {
  private final Graph graph;
  private final BiConsumer<DataItem<?>, HashFunction<DataItem<?>>> router;
  private final Consumer<DataItem<?>> barrier;
  private final ActorContext context;

  private final Map<String, VertexJoba> jobMapping = new HashMap<>();
  private final GraphMaterialization materialization;

  public GraphMaterializer(Graph graph,
                           BiConsumer<DataItem<?>, HashFunction<DataItem<?>>> router,
                           Consumer<DataItem<?>> barrier,
                           ActorContext context) {
    this.graph = graph;
    this.router = router;
    this.barrier = barrier;
    this.context = context;

    buildJobMapping(graph.sink(), null);
    materialization = new GraphMaterialization() {
      @Override
      public Consumer<DataItem<?>> sourceInput() {
        //noinspection unchecked
        return jobMapping.get(graph.source().id());
      }

      @Override
      public BiConsumer<Destination, DataItem<?>> destinationInput() {
        //noinspection unchecked
        return (destination, dataItem) -> jobMapping.get(destination.vertexId).accept(dataItem);
      }

      @Override
      public Consumer<GlobalTime> minTimeInput() {
        return globalTime -> jobMapping.values().forEach(vertexJoba -> vertexJoba.onMinTime(globalTime));
      }

      @Override
      public Runnable commitInput() {
        return () -> jobMapping.values().forEach(VertexJoba::onCommit);
      }
    };
  }

  public GraphMaterialization materialization() {
    return materialization;
  }

  @Override
  public void close() {
    jobMapping.values().forEach(VertexJoba::close);
  }

  @SuppressWarnings("unchecked")
  private void buildJobMapping(Graph.Vertex currentVertex, VertexJoba outputJoba) {
    if (jobMapping.containsKey(currentVertex.id())) {
      final BroadcastJoba<?> broadcastJoba = (BroadcastJoba<?>) currentVertex;
      broadcastJoba.addSink(outputJoba);
    } else {
      final VertexJoba<?> currentJoba;
      if (currentVertex instanceof Sink) {
        currentJoba = barrier::accept;
      } else if (currentVertex instanceof FlameMap) {
        currentJoba = new MapJoba((FlameMap) currentVertex, outputJoba);
      } else if (currentVertex instanceof Grouping) {
        currentJoba = new GroupingJoba((Grouping) currentVertex, outputJoba);
      } else if (currentVertex instanceof Source) {
        currentJoba = new SourceJoba(outputJoba);
      } else {
        throw new RuntimeException("Invalid vertex type");
      }

      final VertexJoba actorJoba = new ActorVertexJoba(currentJoba, context);
      if (graph.isBroadcast(currentVertex)) {
        final BroadcastJoba<?> broadcastJoba = new BroadcastJoba<>();
        broadcastJoba.addSink(actorJoba);
        jobMapping.put(currentVertex.id(), broadcastJoba);
      } else {
        jobMapping.put(currentVertex.id(), actorJoba);
      }

      final VertexJoba currentAsNext;
      if (currentVertex instanceof Grouping) {
        currentAsNext = dataItem -> router.accept((DataItem<?>) dataItem, ((Grouping) currentVertex).hash());
      } else {
        currentAsNext = actorJoba;
      }
      graph.inputs(currentVertex).forEach(vertex -> buildJobMapping(vertex, currentAsNext));
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
  }
}
