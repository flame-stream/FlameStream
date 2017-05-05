package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.ack.Ack;
import com.spbsu.datastream.core.ack.FrontReport;
import com.spbsu.datastream.core.ack.MinTimeUpdate;
import com.spbsu.datastream.core.graph.InPort;

import java.util.Map;

final class TickLocalRouter extends LoggingActor {
  private final Map<InPort, ActorRef> routingTable;
  private final ActorRef acker;

  private TickLocalRouter(final Map<InPort, ActorRef> routingTable) {
    this(routingTable, null);
  }

  private TickLocalRouter(final Map<InPort, ActorRef> routingTable, final ActorRef acker) {
    this.routingTable = routingTable;
    this.acker = acker;
  }

  public static Props props(final Map<InPort, ActorRef> routingTable) {
    return Props.create(TickLocalRouter.class, routingTable);
  }

  public static Props props(final Map<InPort, ActorRef> routingTable, final ActorRef acker) {
    return Props.create(TickLocalRouter.class, routingTable, acker);
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
    } else if (message instanceof Ack || message instanceof FrontReport) {
      this.acker.tell(message, ActorRef.noSender());
    } else if (message instanceof MinTimeUpdate) {
      this.routingTable.values().forEach(atomic -> atomic.tell(message, ActorRef.noSender()));
    } else {
      this.unhandled(message);
    }
  }
}
