package com.spbsu.datastream.core.tick;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import gnu.trove.map.TLongObjectMap;
import scala.Option;

import java.util.Optional;

public final class TickLocalRouter extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());
  private final TLongObjectMap<ActorRef> routingTable;

  private TickLocalRouter(final TLongObjectMap<ActorRef> routingTable) {
    super();
    this.routingTable = routingTable;
  }

  public static Props props(final TLongObjectMap<ActorRef> routingTable) {
    return Props.create(TickLocalRouter.class, routingTable);
  }

  @Override
  public void preStart() throws Exception {
    this.LOG.info("Starting...");
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    this.LOG.info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    this.LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof AddressedMessage) {
      final AddressedMessage addressedMessage = (AddressedMessage) message;
      final ActorRef route = Optional.ofNullable(this.routingTable.get(addressedMessage.port()))
              .orElse(this.context().system().deadLetters());
      route.tell(message, this.self());
    } else {
      this.unhandled(message);
    }
  }
}
