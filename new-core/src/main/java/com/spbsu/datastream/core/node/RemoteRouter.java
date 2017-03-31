package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.tick.AddressedMessage;
import scala.Option;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class RemoteRouter extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(context().system(), self());

  private final Map<HashRange, ActorSelection> routingTable;

  private RemoteRouter(final Map<HashRange, ActorSelection> routingTable) {
    this.routingTable = routingTable;
  }

  public static Props props(final Map<HashRange, ActorSelection> hashMapping) {
    return Props.create(RemoteRouter.class, new HashMap<>(hashMapping));
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
    LOG.debug("Received: {}", message);

    if (message instanceof AddressedMessage) {
      final AddressedMessage addressedMessage = (AddressedMessage) message;

      if (addressedMessage.isBroadcast()) {
        routingTable.values().forEach(a -> a.tell(message, self()));
      } else {
        final int hash = addressedMessage.hash();
        final ActorSelection recipient = remoteDispatcherFor(hash);
        recipient.tell(message, ActorRef.noSender());
      }
    } else {
      unhandled(message);
    }
  }

  private ActorSelection remoteDispatcherFor(final int hash) {
    return routingTable.entrySet().stream().filter((e) -> e.getKey().isIn(hash))
            .map(Map.Entry::getValue).findAny().orElseThrow(NoSuchElementException::new);
  }
}
