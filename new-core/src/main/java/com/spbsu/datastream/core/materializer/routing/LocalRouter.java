package com.spbsu.datastream.core.materializer.routing;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.materializer.AddressedMessage;

import java.util.Map;

public class LocalRouter extends UntypedActor {
  private final Map<InPort, ActorRef> routingTable;

  private LocalRouter(final Map<InPort, ActorRef> routingTable) {
    this.routingTable = routingTable;
  }

  public static Props props(final Map<InPort, ActorRef> routingTable) {
    return Props.create(LocalRouter.class, routingTable);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof AddressedMessage) {
      final ActorRef route = routingTable.getOrDefault(((AddressedMessage) message).port(), context().system().deadLetters());
      route.tell(message, self());
    } else {
      unhandled(message);
    }
  }
}
