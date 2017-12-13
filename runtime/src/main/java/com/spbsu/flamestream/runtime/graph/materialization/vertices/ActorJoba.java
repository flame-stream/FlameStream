package com.spbsu.flamestream.runtime.graph.materialization.vertices;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.UUID;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class ActorJoba implements Joba {
  private final ActorContext context;
  private final ActorRef vertexActor;

  ActorJoba(Joba joba, ActorContext context) {
    this.context = context;
    vertexActor = context.actorOf(InnerActor.props(joba), "ActorJoba_" + UUID.randomUUID());
  }

  @Override
  public boolean isAsync() {
    return true;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    vertexActor.tell(dataItem, context.self());
  }


  private static class InnerActor extends LoggingActor {
    private final Joba joba;

    private InnerActor(Joba joba) {
      this.joba = joba;
    }

    public static Props props(Joba joba) {
      return Props.create(InnerActor.class, joba);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(DataItem.class, this::onDataItem)
              .build();
    }

    private void onDataItem(DataItem dataItem) {
      joba.accept(dataItem, true);
    }
  }
}
