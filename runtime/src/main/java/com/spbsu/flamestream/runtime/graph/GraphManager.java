package com.spbsu.flamestream.runtime.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.runtime.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.graph.api.AddressedItem;
import com.spbsu.flamestream.runtime.graph.api.Commit;
import com.spbsu.flamestream.runtime.graph.vertices.ActorVertexJoba;
import com.spbsu.flamestream.runtime.graph.vertices.VertexJoba;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.commons.lang.math.IntRange;
import org.jooq.lambda.Unchecked;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class GraphManager extends LoggingActor {
  private final Graph graph;
  private final ActorRef acker;
  private final ClusterConfig config;

  private Map<String, ActorVertexJoba> materialization;

  private GraphManager(Graph graph, ActorRef acker, ClusterConfig config) {
    this.graph = graph;
    this.acker = acker;
    this.config = config;
  }

  public static Props props(Graph logicalGraph, ActorRef acker, ClusterConfig config) {
    return Props.create(GraphManager.class, logicalGraph, acker, config);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(DataItem.class, this::accept)
            .match(AddressedItem.class, this::inject)
            .match(MinTimeUpdate.class, this::onMinTimeUpdate)
            .match(Commit.class, commit -> onCommit())
            .build();
  }

  @Override
  public void preStart() throws Exception {
    final Map<String, ActorRef> barriers = new HashMap<>();
    config.nodes().forEach(nodeConfig -> {
      final ActorRef b = AwaitResolver.syncResolve(nodeConfig.nodePath().child("barrier"), context());
      barriers.put(nodeConfig.id(), b);
    });
    final Map<IntRange, ActorRef> managers = new HashMap<>();
    config.nodes().forEach(nodeConfig -> {
      final ActorRef manager = AwaitResolver.syncResolve(nodeConfig.nodePath().child("graph"), context());
      managers.put(nodeConfig.range().asRange(), manager);
    });

    materialization = new HashMap<>();
    // TODO: 30.11.2017 build graph
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
