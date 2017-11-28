package com.spbsu.flamestream.runtime.node.graph.materialization.vertices;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.Commit;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class VertexAkkaMaterialization implements VertexMaterialization {
  private final ActorContext context;
  private final ActorRef vertexActor;

  public VertexAkkaMaterialization(VertexMaterialization materialization, ActorContext context) {
    this.context = context;
    vertexActor = context.actorOf(VertexActor.props(materialization));
  }

  @Override
  public void onMinTime(GlobalTime globalTime) {
    vertexActor.tell(globalTime, context.self());
  }

  @Override
  public void onCommit() {
    vertexActor.tell(new Commit(), context.self());
  }

  @Override
  public void accept(DataItem<?> dataItem) {
    vertexActor.tell(dataItem, context.self());
  }
}
