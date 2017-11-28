package com.spbsu.flamestream.runtime.node.graph.materialization.vertices;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.AddressedItem;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.Commit;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class VertexActor extends LoggingActor {
  private final VertexMaterialization materialization;
  private final Stream<String> nextVertices;

  private VertexActor(VertexMaterialization materialization, Stream<String> nextVertices) {
    this.materialization = materialization;
    this.nextVertices = nextVertices;
  }

  public static Props props(VertexMaterialization materialization, Stream<String> nextVertices) {
    return Props.create(VertexActor.class, materialization, nextVertices);
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(DataItem.class, this::onDataItem)
            .match(GlobalTime.class, this::onGlobalTime)
            .match(Commit.class, commit -> onCommit())
            .build();
  }

  private void onDataItem(DataItem<?> dataItem) {
    materialization.apply(dataItem).forEach(di ->
            nextVertices.forEach(vertexId -> context().self().tell(new AddressedItem(di, vertexId), self())));
  }

  private void onGlobalTime(GlobalTime globalTime) {
    materialization.onMinGTimeUpdate(globalTime);
  }

  private void onCommit() {
    materialization.onCommit();
  }
}
