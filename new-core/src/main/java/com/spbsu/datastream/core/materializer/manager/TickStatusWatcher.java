package com.spbsu.datastream.core.materializer.manager;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import com.spbsu.AddressUtils;
import org.apache.zookeeper.*;
import org.jooq.lambda.Unchecked;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TickStatusWatcher extends UntypedActor {
  private final ZooKeeper zk;
  private final long tick;
  private final ActorRef subscriber;

  private Set<InetSocketAddress> members;

  private TickStatusWatcher(final ZooKeeper zk, final long tick, final ActorRef subscriber) {
    this.zk = zk;
    this.tick = tick;
    this.subscriber = subscriber;
  }

  public static Props props(final ZooKeeper zk, final long tick, final ActorRef subscriber) {
    return Props.create(TickStatusWatcher.class, zk, tick, subscriber);
  }

  private static Set<InetSocketAddress> mapChildren(final List<String> children) {
    return children.stream().map(Unchecked.function(AddressUtils::addressFromString)).collect(Collectors.toSet());
  }

  @Override
  public void preStart() throws Exception {
    final String path = "/tick/" + tick + "/member";
    this.members = fetchMembers(path);
  }

  private Set<InetSocketAddress> fetchMembers(final String path) throws KeeperException, InterruptedException {
    return mapChildren(zk.getChildren(path, selfWatcher()));
  }

  @Override
  public void onReceive(final Object message) throws Throwable {
    if (message instanceof Set) {
      if (message.equals(members)) {
        subscriber.tell(new TickGraphManagerApi.TickStarted(), self());
      }
    } else if (message instanceof WatchedEvent) {
      final WatchedEvent event = ((WatchedEvent) message);
      if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
        final String path = "/tick/" + tick + "/ready";
        zk.getChildren(path, selfWatcher(), selfCallback(), null);
      }
    }
  }

  private Watcher selfWatcher() {
    return event -> self().tell(event, null);
  }

  private AsyncCallback.ChildrenCallback selfCallback() {
    return (rc, p, ctx, children) -> {
      self().tell(mapChildren(children), null);
    };
  }
}
