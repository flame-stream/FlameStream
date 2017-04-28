package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.graph.InPort;

import java.util.Map;

public final class TickLocalRouter extends LoggingActor {
  private final Map<InPort, ActorRef> routingTable;

  private TickLocalRouter(final Map<InPort, ActorRef> routingTable) {
    this.routingTable = routingTable;
  }

  public static Props props(final Map<InPort, ActorRef> routingTable) {
    return Props.create(TickLocalRouter.class, routingTable);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof PortBindDataItem) {
      final PortBindDataItem portBindDataItem = (PortBindDataItem) message;
      final ActorRef route = this.routingTable.get(portBindDataItem.inPort());

      if (route != null) {
        route.tell(message, ActorRef.noSender());
      } else {
        this.unhandled(message);
      }
    } else {
      this.unhandled(message);
    }
  }
}
