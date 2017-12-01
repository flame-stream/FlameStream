package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.ActorVertexJoba;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.BroadcastJoba;
import com.spbsu.flamestream.runtime.graph.materialization.vertices.ConsumerJoba;
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
public class ActorPerNodeMaterializer implements GraphMaterializer {
  private final Graph graph;
  private final BiConsumer<DataItem<?>, HashFunction<DataItem<?>>> router;
  private final Consumer<DataItem<?>> barrier;
  private final ActorContext context;
  private final Map<String, VertexJoba> materialization = new HashMap<>();

  public ActorPerNodeMaterializer(Graph graph,
                                  BiConsumer<DataItem<?>, HashFunction<DataItem<?>>> router,
                                  Consumer<DataItem<?>> barrier,
                                  ActorContext context) {
    this.graph = graph;
    this.router = router;
    this.barrier = barrier;
    this.context = context;
  }

  @SuppressWarnings("unchecked")
  private void buildMaterialization(Graph.Vertex currentVertex, VertexJoba outputJoba) {
    if (materialization.containsKey(currentVertex.id())) {
      final BroadcastJoba<?> broadcastJoba = (BroadcastJoba<?>) currentVertex;
      broadcastJoba.addSink(outputJoba);
    } else {
      final VertexJoba currentJoba;
      if (currentVertex instanceof Sink) {
        currentJoba = new ConsumerJoba(dataItem -> barrier.accept((DataItem<?>) dataItem));
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
        materialization.put(currentVertex.id(), broadcastJoba);
      } else {
        materialization.put(currentVertex.id(), actorJoba);
      }

      final VertexJoba currentAsNext;
      if (currentVertex instanceof Grouping) {
        currentAsNext = new ConsumerJoba(dataItem -> router.accept(
                (DataItem<?>) dataItem,
                ((Grouping) currentVertex).hash())
        );
      } else {
        currentAsNext = actorJoba;
      }
      graph.inputs(currentVertex).forEach(vertex -> buildMaterialization(vertex, currentAsNext));
    }
  }

  @Override
  public Map<String, VertexJoba> materialize() {
    if (!materialization.isEmpty()) {
      materialization.clear();
    }
    buildMaterialization(graph.sink(), null);
    return materialization;
  }
}
