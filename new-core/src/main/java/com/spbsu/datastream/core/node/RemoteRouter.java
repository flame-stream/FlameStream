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

public final class RemoteRouter extends UntypedActor {
  private final LoggingAdapter LOG = Logging.getLogger(this.context().system(), this.self());

  private final Map<HashRange, ActorSelection> routingTable;

  private RemoteRouter(final Map<HashRange, ActorSelection> routingTable) {
    super();
    this.routingTable = routingTable;
  }

  public static Props props(final Map<HashRange, ActorSelection> hashMapping) {
    return Props.create(RemoteRouter.class, new HashMap<>(hashMapping));
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
    this.LOG.debug("Received: {}", message);

    if (message instanceof AddressedMessage) {
      final AddressedMessage addressedMessage = (AddressedMessage) message;

      if (addressedMessage.isBroadcast()) {
        this.routingTable.values().forEach(a -> a.tell(message, this.self()));
      } else {
        final int hash = addressedMessage.hash();
        final ActorSelection recipient = this.remoteDispatcherFor(hash);
        recipient.tell(message, ActorRef.noSender());
      }
    } else {
      this.unhandled(message);
    }
  }

  private ActorSelection remoteDispatcherFor(final int hash) {
    return this.routingTable.entrySet().stream().filter(e -> e.getKey().isIn(hash))
            .map(Map.Entry::getValue).findAny().orElseThrow(NoSuchElementException::new);
  }
}
