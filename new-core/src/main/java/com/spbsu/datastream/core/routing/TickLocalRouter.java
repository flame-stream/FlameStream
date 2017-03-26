package com.spbsu.datastream.core.routing;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.materializer.AddressedMessage;
import gnu.trove.map.TLongObjectMap;
import scala.Option;

import java.util.Optional;

public class TickLocalRouter extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());
  private final TLongObjectMap<ActorRef> routingTable;

  private TickLocalRouter(final TLongObjectMap<ActorRef> routingTable) {
    this.routingTable = routingTable;
  }

  public static Props props(final TLongObjectMap<ActorRef> routingTable) {
    return Props.create(TickLocalRouter.class, routingTable);
  }

  @Override
  public void preStart() throws Exception {
    LOG.info("Starting...");
    super.preStart();
  }

  @Override
  public void postStop() throws Exception {
    LOG.info("Stopped");
    super.postStop();
  }

  @Override
  public void preRestart(final Throwable reason, final Option<Object> message) throws Exception {
    LOG.error("Restarting, reason: {}, message: {}", reason, message);
    super.preRestart(reason, message);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof AddressedMessage) {
      final AddressedMessage addressedMessage = (AddressedMessage) message;
      final ActorRef route = Optional.ofNullable(routingTable.get(addressedMessage.port()))
              .orElse(context().system().deadLetters());
      route.tell(message, self());
    } else {
      unhandled(message);
    }
  }
}
