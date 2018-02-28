package com.spbsu.flamestream.runtime.edge.akka;

import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.runtime.edge.EdgeContext;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;

import java.util.ArrayList;
import java.util.List;

public class AkkaRear implements Rear {
  private final ActorRef innerActor;

  public AkkaRear(EdgeContext edgeContext, ActorRefFactory refFactory) {
    this.innerActor = refFactory.actorOf(InnerActor.props(), edgeContext.edgeId().nodeId() + "-inner");
  }

  @Override
  public void accept(DataItem item) {
    innerActor.tell(item, ActorRef.noSender());
  }

  private static class InnerActor extends LoggingActor {
    private final List<ActorRef> subscribers = new ArrayList<>();

    public static Props props() {
      return Props.create(InnerActor.class);
    }

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(ActorRef.class, e -> {
                subscribers.add(e);
                if (subscribers.size() == 1) {
                  unstashAll();
                }
              })
              .matchAny(d -> {
                if (subscribers.isEmpty()) {
                  stash();
                } else {
                  subscribers.forEach(c -> c.forward(d, context()));
                }
              })
              .build();
    }
  }
}
