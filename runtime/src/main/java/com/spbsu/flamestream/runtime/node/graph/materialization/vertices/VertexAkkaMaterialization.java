package com.spbsu.flamestream.runtime.node.graph.materialization.vertices;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.node.graph.materialization.api.Commit;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class VertexAkkaMaterialization extends VertexMaterialization.Stub {
  private final ActorContext context;
  private final ActorRef vertexActor;

  public VertexAkkaMaterialization(VertexMaterialization materialization, Stream<String> nextVertices, ActorContext context) {
    super(materialization.vertexId());
    this.context = context;
    vertexActor = context.actorOf(VertexActor.props(materialization, nextVertices));
  }

  @Override
  public void onMinGTimeUpdate(GlobalTime globalTime) {
    vertexActor.tell(globalTime, context.self());
  }

  @Override
  public void onCommit() {
    vertexActor.tell(new Commit(), context.self());
  }

  @Override
  public Stream<DataItem<?>> apply(DataItem<?> dataItem) {
    vertexActor.tell(dataItem, context.self());
    return Stream.empty();
  }
}
