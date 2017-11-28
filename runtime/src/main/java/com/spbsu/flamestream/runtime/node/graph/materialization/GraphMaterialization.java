package com.spbsu.flamestream.runtime.node.graph.materialization;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.node.graph.GraphRouter;
import com.spbsu.flamestream.runtime.node.graph.acker.api.MinTimeUpdate;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.AddressedItem;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.Commit;
import com.spbsu.flamestream.runtime.node.graph.materialization.vertices.VertexMaterialization;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class GraphMaterialization extends LoggingActor {
  private final Map<String, VertexMaterialization> materialization = new HashMap<>();

  private GraphMaterialization(Graph graph, GraphRouter router, ActorRef acker, ActorRef barrier) {

  }

  public static Props props(Graph graph, GraphRouter router, ActorRef acker, ActorRef barrier) {
    return Props.create(GraphMaterialization.class, graph, router, acker, barrier);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(AddressedItem.class, this::onAddressedItem)
            .match(MinTimeUpdate.class, this::onMinTimeUpdate)
            .match(Commit.class, commit -> onCommit())
            .build();
  }

  private void onAddressedItem(AddressedItem atomicMessage) {
    final VertexMaterialization nextVertices = materialization.get(atomicMessage.vertexId());
    nextVertices.forEach(materialization -> {
      final Stream<DataItem<?>> out = materialization.apply(atomicMessage.item());
      out.forEach(dataItem -> onAddressedItem(new AddressedItem(dataItem, materialization.vertexId())));
    });
  }

  private void onMinTimeUpdate(MinTimeUpdate minTimeUpdate) {
    materialization.values()
            .stream()
            .flatMap(stream -> stream)
            .forEach(materialization -> materialization.onMinGTimeUpdate(minTimeUpdate.minTime()));
  }

  private void onCommit() {
    materialization.values()
            .stream()
            .flatMap(stream -> stream)
            .forEach(VertexMaterialization::onCommit);
  }
}
