package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Grouping;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.acker.api.Ack;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.config.ComputationLayout;
import com.spbsu.flamestream.runtime.config.HashRange;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.api.Commit;
import com.spbsu.flamestream.runtime.graph.vertices.ActorVertexJoba;
import com.spbsu.flamestream.runtime.graph.vertices.BroadcastJoba;
import com.spbsu.flamestream.runtime.graph.vertices.ConsumerJoba;
import com.spbsu.flamestream.runtime.graph.vertices.GroupingJoba;
import com.spbsu.flamestream.runtime.graph.vertices.MapJoba;
import com.spbsu.flamestream.runtime.graph.vertices.SourceJoba;
import com.spbsu.flamestream.runtime.graph.vertices.VertexJoba;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.jooq.lambda.Unchecked;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class GraphManager extends LoggingActor {
  private final String nodeId;
  private final Graph graph;
  private final ActorRef acker;
  private final ComputationLayout layout;
  private final BiConsumer<DataItem<?>, ActorRef> barrier;

  private final Map<String, ActorRef> managerRefs = new HashMap<>();
  private final Map<String, VertexJoba> materialization = new HashMap<>();

  private GraphManager(String nodeId,
                       Graph graph,
                       ActorRef acker,
                       ComputationLayout layout,
                       BiConsumer<DataItem<?>, ActorRef> barrier) {
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
                            BiConsumer<DataItem<?>, ActorRef> barrier) {
    return Props.create(GraphManager.class, nodeId, graph, acker, layout, barrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Map.class, managers -> {
              log().info("Finishing constructor");
              //noinspection unchecked
              managerRefs.putAll(managers);
              buildMaterialization(graph.sink(), null);

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
            .match(Commit.class, commit -> onCommit())
            .match(GlobalTime.class, gt -> acker.tell(new Heartbeat(gt, gt.front(), nodeId), self()))
            .build();
  }

  @Override
  public void postStop() {
    materialization.values().forEach(Unchecked.consumer(AutoCloseable::close));
  }

  private void accept(DataItem<?> dataItem) {
    //noinspection unchecked
    materialization.get(graph.source().id()).accept(dataItem);
    ack(dataItem);
  }

  private void inject(AddressedItem addressedItem) {
    //noinspection unchecked
    materialization.get(addressedItem.vertexId()).accept(addressedItem.item());
    ack(addressedItem.item());
  }

  private void onMinTimeUpdate(MinTimeUpdate minTimeUpdate) {
    materialization.values().forEach(materialization -> materialization.onMinTime(minTimeUpdate.minTime()));
  }

  private void onCommit() {
    materialization.values().forEach(VertexJoba::onCommit);
  }

  @SuppressWarnings("unchecked")
  private void buildMaterialization(Graph.Vertex currentVertex, VertexJoba outputJoba) {
    if (materialization.containsKey(currentVertex.id())) {
      final BroadcastJoba<?> broadcastJoba = (BroadcastJoba<?>) currentVertex;
      broadcastJoba.addSink(outputJoba);
    } else {
      final VertexJoba currentJoba;
      if (currentVertex instanceof Sink) {
        currentJoba = new ConsumerJoba(dataItem -> barrier.accept((DataItem<?>) dataItem, self()));
      } else if (currentVertex instanceof FlameMap) {
        currentJoba = new MapJoba((FlameMap) currentVertex, outputJoba);
      } else if (currentVertex instanceof Grouping) {
        currentJoba = new GroupingJoba((Grouping) currentVertex, outputJoba);
      } else if (currentVertex instanceof Source) {
        currentJoba = new SourceJoba(outputJoba);
      } else {
        throw new RuntimeException("Invalid vertex type");
      }

      final VertexJoba actorJoba = new ActorVertexJoba(currentJoba, context());
      if (graph.isBroadcast(currentVertex)) {
        final BroadcastJoba<?> broadcastJoba = new BroadcastJoba<>();
        broadcastJoba.addSink(actorJoba);
        materialization.put(currentVertex.id(), broadcastJoba);
      } else {
        materialization.put(currentVertex.id(), actorJoba);
      }

      final VertexJoba currentAsNext;
      if (currentVertex instanceof Grouping) {
        currentAsNext = new ConsumerJoba(dataItem -> routerSink(((Grouping) currentVertex).hash()).accept(dataItem));
      } else {
        currentAsNext = actorJoba;
      }
      graph.inputs(currentVertex).forEach(vertex -> buildMaterialization(vertex, currentAsNext));
    }
  }

  private Consumer<DataItem<?>> routerSink(HashFunction<DataItem<?>> hashFunction) {
    // TODO: 01.12.2017 we lost optimization (acking once flatmap results)
    return dataItem -> {
      final int hash = hashFunction.applyAsInt(dataItem);
      for (Map.Entry<String, HashRange> entry : layout.ranges().entrySet()) {
        if (entry.getValue().from() <= hash && hash < entry.getValue().to()) {
          managerRefs.get(entry.getKey()).tell(dataItem, self());
          ack(dataItem);
          return;
        }
      }
      throw new IllegalStateException("Hash ranges doesn't cover Integer space");
    };
  }

  private void ack(DataItem<?> dataItem) {
    acker.tell(new Ack(dataItem.meta().globalTime(), dataItem.xor()), self());
  }
}
