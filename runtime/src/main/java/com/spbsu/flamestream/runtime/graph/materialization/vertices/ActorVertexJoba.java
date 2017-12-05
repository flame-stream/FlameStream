package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Commit;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.UUID;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class ActorVertexJoba<T> implements VertexJoba<T> {
  private final ActorContext context;
  private final ActorRef vertexActor;
  private final Consumer<DataItem<?>> acker;

  public ActorVertexJoba(VertexJoba<T> joba, Consumer<DataItem<?>> acker, ActorContext context) {
    this.context = context;
    this.acker = acker;
    vertexActor = context.actorOf(InnerActor.props(joba, acker), "ActorJoba_" + UUID.randomUUID());
  }

  @Override
  public boolean isAsync() {
    return true;
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
    //acker.accept(dataItem);
    //System.out.println("Acking for sebding from " + toString() + " with " + dataItem.xor());
    vertexActor.tell(dataItem, context.self());
  }

  @Override
  public void close() {
    context.stop(vertexActor);
  }

  private static class InnerActor<T> extends LoggingActor {
    private final VertexJoba<T> joba;
    private final Consumer<DataItem<?>> acker;

    private InnerActor(VertexJoba<T> joba, Consumer<DataItem<?>> acker) {
      this.joba = joba;
      this.acker = acker;
    }

    public static <T> Props props(VertexJoba<T> joba, Consumer<DataItem<?>> acker) {
      return Props.create(InnerActor.class, joba, acker);
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
      System.out.println("Acking for acc from " + toString() + " with " + dataItem.xor());
      acker.accept(dataItem);
    }

    private void onGlobalTime(GlobalTime globalTime) {
      joba.onMinTime(globalTime);
    }

    private void onCommit() {
      joba.onCommit();
    }
  }
}
