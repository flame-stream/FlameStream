package com.spbsu.flamestream.runtime.graph.materialization.jobas;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.graph.materialization.api.Commit;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.UUID;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class ActorJoba<T> implements Joba<T> {
  private final ActorContext context;
  private final ActorRef vertexActor;

  public ActorJoba(Joba<T> joba, ActorContext context) {
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
  public void close() throws Exception {
    vertexActor.tell(PoisonPill.getInstance(), context.self());
  }

  private static class InnerActor<T> extends LoggingActor {
    private final Joba<T> joba;

    private InnerActor(Joba<T> joba) {
      this.joba = joba;
    }

    public static <T> Props props(Joba<T> joba) {
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
