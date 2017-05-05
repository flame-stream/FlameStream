package com.spbsu.datastream.core.node;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.range.AddressedMessage;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;


public final class RootRouter extends LoggingActor {
  private final Map<HashRange, ActorSelection> routingTable;

  private RootRouter(final Map<HashRange, InetSocketAddress> ranges) {
    this.routingTable = this.rangeRouters(ranges);
  }

  public static Props props(final Map<HashRange, InetSocketAddress> hashMapping) {
    return Props.create(RootRouter.class, hashMapping);
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    this.LOG().debug("Received: {}", message);

    if (message instanceof AddressedMessage) {
      final AddressedMessage<?> addressedMessage = (AddressedMessage<?>) message;
      if (!addressedMessage.isBroadcast()) {
        final int hash = addressedMessage.hash();
        final ActorSelection recipient = this.rangeRouterFor(hash);
        recipient.tell(message, ActorRef.noSender());
      } else {
        this.routingTable.values().forEach(as -> as.tell(message, ActorRef.noSender()));
      }
    } else {
      this.unhandled(message);
    }
  }

  private ActorSelection rangeRouterFor(final int hash) {
    return this.routingTable.entrySet().stream().filter(e -> e.getKey().contains(hash))
            .map(Map.Entry::getValue).findAny().orElseThrow(NoSuchElementException::new);
  }

  private Map<HashRange, ActorSelection> rangeRouters(final Map<HashRange, InetSocketAddress> mappings) {
    return mappings.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> this.rangeRouter(e.getValue(), e.getKey())));
  }

  private ActorSelection rangeRouter(final InetSocketAddress socketAddress, final HashRange range) {
    final ActorPath rangeRouterPath = MyPaths.rangeRouter(socketAddress, range);
    return this.context().system().actorSelection(rangeRouterPath);
  }
}
