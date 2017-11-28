package com.spbsu.flamestream.runtime.node.graph.materialization.vertices;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.Commit;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public class VertexActor extends LoggingActor {
  private final VertexMaterialization materialization;

  private VertexActor(VertexMaterialization materialization) {
    this.materialization = materialization;
  }

  public static Props props(VertexMaterialization materialization) {
    return Props.create(VertexActor.class, materialization);
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
    materialization.accept(dataItem);
  }

  private void onGlobalTime(GlobalTime globalTime) {
    materialization.onMinTime(globalTime);
  }

  private void onCommit() {
    materialization.onCommit();
  }
}
