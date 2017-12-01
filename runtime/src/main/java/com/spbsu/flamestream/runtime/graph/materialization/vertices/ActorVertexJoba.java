package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.graph.api.Commit;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.UUID;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class ActorVertexJoba<T> extends VertexJoba.Stub<T> {
  private final ActorContext context;
  private final ActorRef vertexActor;

  public ActorVertexJoba(VertexJoba<T> joba, ActorContext context) {
    this.context = context;
    vertexActor = context.actorOf(InnerActor.props(joba), "ActorJoba_" + UUID.randomUUID());
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
  public void accept(DataItem<T> dataItem) {
    vertexActor.tell(dataItem, context.self());
  }

  @Override
  public void close() {
    vertexActor.tell(PoisonPill.getInstance(), context.self());
  }

  private static class InnerActor<T> extends LoggingActor {
    private final VertexJoba<T> joba;

    private InnerActor(VertexJoba<T> joba) {
      this.joba = joba;
    }

    public static <T> Props props(VertexJoba<T> joba) {
      return Props.create(InnerActor.class, joba);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(DataItem.class, this::onDataItem)
              .match(GlobalTime.class, this::onGlobalTime)
              .match(Commit.class, commit -> onCommit())
              .build();
    }

    private void onDataItem(DataItem<T> dataItem) {
      joba.accept(dataItem);
    }

    private void onGlobalTime(GlobalTime globalTime) {
      joba.onMinTime(globalTime);
    }

    private void onCommit() {
      joba.onCommit();
    }
  }
}
