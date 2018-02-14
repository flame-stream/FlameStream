package com.spbsu.flamestream.runtime.graph.materialization;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.acker.api.Heartbeat;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.UUID;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public class ActorJoba implements Joba, MinTimeHandler {
  private final ActorContext context;
  private final ActorRef vertexActor;
  private final boolean minTimeHandler;

  public ActorJoba(Joba joba, ActorRef acker) {
    context = ((Joba.Stub) joba).context;
    minTimeHandler = joba instanceof MinTimeHandler;
    vertexActor = context.actorOf(InnerActor.props(joba, acker), "ActorJoba_" + UUID.randomUUID());
  }

  @Override
  public boolean isAsync() {
    return true;
  }

  @Override
  public void accept(DataItem dataItem, boolean fromAsync) {
    vertexActor.tell(dataItem, context.self());
  }

  @Override
  public void onMinTime(GlobalTime minTime) {
    if (minTimeHandler) {
      vertexActor.tell(minTime, context.self());
    }
  }

  public void onHeartBeat(Heartbeat heartbeat) {
    vertexActor.tell(heartbeat, context.self());
  }

  public void addFront(EdgeId edgeId, ActorRef actorRef) {
    vertexActor.tell(new AddFront(edgeId, actorRef), context.self());
  }

  private static class InnerActor extends LoggingActor {
    private final Joba joba;
    private final ActorRef acker;

    private InnerActor(Joba joba, ActorRef acker) {
      this.joba = joba;
      this.acker = acker;
    }

    public static Props props(Joba joba, ActorRef acker) {
      return Props.create(InnerActor.class, joba, acker);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(DataItem.class, this::onDataItem)
              .match(GlobalTime.class, this::onMinTime)
              .match(Heartbeat.class, heartbeat -> acker.forward(heartbeat, context()))
              .match(AddFront.class, addFront -> ((SourceJoba)joba).addFront(addFront.edgeId(), addFront.actorRef()))
              .build();
    }

    private void onDataItem(DataItem dataItem) {
      if (joba instanceof SourceJoba) {
        joba.accept(dataItem, false);
      } else {
        joba.accept(dataItem, true);
      }
    }

    private void onMinTime(GlobalTime minTime) {
      ((MinTimeHandler) joba).onMinTime(minTime);
    }
  }

  private static class AddFront {
    private final EdgeId edgeId;
    private final ActorRef actorRef;

    private AddFront(EdgeId edgeId, ActorRef actorRef) {
      this.edgeId = edgeId;
      this.actorRef = actorRef;
    }

    public EdgeId edgeId() {
      return edgeId;
    }

    public ActorRef actorRef() {
      return actorRef;
    }
  }
}
