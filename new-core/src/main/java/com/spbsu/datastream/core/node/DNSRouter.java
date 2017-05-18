package com.spbsu.datastream.core.node;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.RootActorPath;
import com.spbsu.datastream.core.LoggingActor;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.stream.Collectors;

final class DNSRouter extends LoggingActor {
  private final Map<Integer, ActorSelection> dns;
  private final ActorRef localRouter;
  private final int localId;

  public static Props props(Map<Integer, InetSocketAddress> dns,
                            ActorRef localRouter,
                            int localId) {
    return Props.create(DNSRouter.class, dns, localRouter, localId);
  }

  private DNSRouter(Map<Integer, InetSocketAddress> dns,
                    ActorRef localRouter,
                    int localId) {
    this.dns = dns.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> this.selectionFor(e.getKey(), e.getValue())));
    this.localRouter = localRouter;
    this.localId = localId;
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder().match(UnresolvedMessage.class, this::onUnresolved).build();
  }


  private void onUnresolved(UnresolvedMessage<?> message) {
    this.LOG().debug("Received: {}", message);
    if (message.destination() == this.localId) {
      this.localRouter.tell(message.payload(), this.sender());
    } else {
      this.sendRemote(message);
    }
  }

  private void sendRemote(UnresolvedMessage<?> message) {
    if (message.isBroadcast()) {
      this.dns.forEach((key, value) -> value.tell(new UnresolvedMessage<>(key, message.payload()), this.sender()));
    } else {
      final ActorSelection receiver = this.dns.get(message.destination());
      if (receiver != null) {
        receiver.tell(message, this.sender());
      } else {
        this.unhandled(message);
      }
    }
  }

  private ActorSelection selectionFor(int id, InetSocketAddress address) {
    // TODO: 5/8/17 Properly resolve ActorRef
    final Address add = Address.apply("akka.tcp", "worker", address.getAddress().getHostName(), address.getPort());
    final ActorPath path = RootActorPath.apply(add, "/")
            .$div("user")
            .$div(String.valueOf(id))
            .$div("dns");
    return this.context().actorSelection(path);
  }
}
