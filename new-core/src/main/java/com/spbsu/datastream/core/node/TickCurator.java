package com.spbsu.datastream.core.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import com.spbsu.datastream.core.configuration.KryoInfoSerializer;
import com.spbsu.datastream.core.configuration.TickInfoSerializer;
import com.spbsu.datastream.core.tick.TickInfo;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TickCurator extends LoggingActor {
  private final TickInfoSerializer serializer = new KryoInfoSerializer();
  private final ZooKeeper zooKeeper;
  private final ActorRef notify;

  private final Map<Long, TickInfo> seenTicks = new HashMap<>();

  private TickCurator(final ZooKeeper zooKeeper, final ActorRef notify) {
    this.zooKeeper = zooKeeper;
    this.notify = notify;
  }

  public static Props props(final ZooKeeper zooKeeper, final ActorRef notify) {
    return Props.create(TickCurator.class, zooKeeper, notify);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    this.fetchTicks();
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder().match(WatchedEvent.class, this::onEvent).build();
  }

  private void fetchTicks() throws KeeperException, InterruptedException {
    final List<String> ticks = this.zooKeeper.getChildren("/ticks", this.selfWatcher());

    for (final String tick : ticks) {
      if (!this.seenTicks.containsKey(Long.valueOf(tick))) {
        final byte[] data = this.zooKeeper.getData("/ticks/" + tick, false, null);
        final TickInfo tickInfo = this.serializer.deserialize(data);
        this.seenTicks.putIfAbsent(Long.valueOf(tick), tickInfo);
        this.notify.tell(tickInfo, ActorRef.noSender());
      }
    }
  }

  private Watcher selfWatcher() {
    return event -> this.self().tell(event, this.self());
  }

  private void onEvent(WatchedEvent event) throws KeeperException, InterruptedException {
    if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
      this.fetchTicks();
    } else {
      this.LOG().warning("Unexpected event {}", event);
    }
  }
}
