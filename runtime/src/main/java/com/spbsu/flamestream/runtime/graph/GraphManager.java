package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.config.ComputationLayout;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.api.Commit;
import com.spbsu.flamestream.runtime.graph.vertices.ActorVertexJoba;
import com.spbsu.flamestream.runtime.graph.vertices.VertexJoba;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.commons.lang.math.IntRange;
import org.jooq.lambda.Unchecked;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class GraphManager extends LoggingActor {
  private final Graph graph;
  private final ActorRef acker;

  private final Map<String, ActorVertexJoba> materialization = new HashMap<>();
  private final BiConsumer<DataItem<?>, ActorRef> barrier;
  private final Map<String, ActorRef> managers = new HashMap<>();
  private final ComputationLayout layout;

  private GraphManager(Graph graph,
                       ActorRef acker,
                       ComputationLayout layout,
                       BiConsumer<DataItem<?>, ActorRef> barrier) {
    this.layout = layout;
    this.graph = graph;
    this.acker = acker;
    this.barrier = barrier;
  }

  public static Props props(Graph graph,
                            ActorRef acker,
                            ComputationLayout layout,
                            BiConsumer<DataItem<?>, ActorRef> barrier) {
    return Props.create(GraphManager.class, graph, acker, layout, barrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(Map.class, managers -> {
              log().info("Finishing constructor");
              managers.putAll(managers);
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
            .build();
  }

  @Override
  public void postStop() {
    materialization.values().forEach(Unchecked.consumer(ActorVertexJoba::close));
  }

  private void accept(DataItem<?> dataItem) {
    //noinspection unchecked
    materialization.get(graph.source().id()).accept(dataItem);
  }

  private void inject(AddressedItem addressedItem) {
    //noinspection unchecked
    materialization.get(addressedItem.vertexId()).accept(addressedItem.item());
  }

  private void onMinTimeUpdate(MinTimeUpdate minTimeUpdate) {
    materialization.values().forEach(materialization -> materialization.onMinTime(minTimeUpdate.minTime()));
  }

  private void onCommit() {
    materialization.values().forEach(VertexJoba::onCommit);
  }

  private Consumer<DataItem<?>> barrierSink(Map<String, ActorRef> barriers) {
    return dataItem -> barriers.get(dataItem.meta().globalTime().front()).tell(dataItem, self());
  }

  private Consumer<DataItem<?>> routerSink(Map<IntRange, ActorRef> managers, HashFunction<DataItem<?>> hashFunction) {
    return dataItem -> {
      final int hash = hashFunction.applyAsInt(dataItem);
      for (Map.Entry<IntRange, ActorRef> entry : managers.entrySet()) {
        if (entry.getKey().containsInteger(hash)) {
          entry.getValue().tell(dataItem, self());
          return;
        }
      }
      throw new IllegalStateException("Hash ranges doesn't cover Integer space");
    };
  }
}
