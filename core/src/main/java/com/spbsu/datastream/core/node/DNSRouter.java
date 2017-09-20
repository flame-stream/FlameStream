package com.spbsu.datastream.core.node;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.RootActorPath;
import com.spbsu.datastream.core.LoggingActor;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.net.InetSocketAddress;
import java.util.Map;

final class DNSRouter extends LoggingActor {
  private final TIntObjectMap<ActorSelection> dns;
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
    this.dns = new TIntObjectHashMap<>();
    dns.forEach((i, addr) -> this.dns.put(i, selectionFor(i, addr)));

    this.localRouter = localRouter;
    this.localId = localId;
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder().match(UnresolvedMessage.class, this::onUnresolved).build();
  }


  private void onUnresolved(UnresolvedMessage<?> message) {
    LOG().debug("Received: {}", message);
    if (message.destination() == localId) {
      localRouter.tell(message.payload(), sender());
    } else {
      sendRemote(message);
    }
  }

  private void sendRemote(UnresolvedMessage<?> message) {
    if (message.isBroadcast()) {
      dns.forEachEntry((key, value) -> {
        value.tell(new UnresolvedMessage<>(key, message.payload()), sender());
        return true;
      });
    } else {
      final ActorSelection receiver = dns.get(message.destination());
      if (receiver != null) {
        receiver.tell(message, sender());
      } else {
        LOG().error("Unknown destanation {}", message.destination());
      }
    }
  }

  private ActorSelection selectionFor(int id, InetSocketAddress address) {
    // TODO: 5/8/17 Properly resolve ActorRef
    final Address add = Address.apply("akka.tcp", "worker", address.getAddress().getHostName(), address.getPort());
    final ActorPath path = RootActorPath.apply(add, "/")
            .$div("user")
            .$div("watcher")
            .$div("concierge")
            .$div("dns");
    return context().actorSelection(path);
  }
}
