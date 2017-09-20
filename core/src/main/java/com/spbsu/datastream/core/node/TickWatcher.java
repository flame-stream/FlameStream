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

public final class TickWatcher extends LoggingActor {
  private final TickInfoSerializer serializer = new KryoInfoSerializer();
  private final ZooKeeper zooKeeper;
  private final ActorRef notify;

  private final Map<Long, TickInfo> seenTicks = new HashMap<>();

  private TickWatcher(ZooKeeper zooKeeper, ActorRef notify) {
    this.zooKeeper = zooKeeper;
    this.notify = notify;
  }

  public static Props props(ZooKeeper zooKeeper, ActorRef notify) {
    return Props.create(TickWatcher.class, zooKeeper, notify);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    fetchTicks();
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder().match(WatchedEvent.class, this::onEvent).build();
  }

  private void fetchTicks() throws KeeperException, InterruptedException {
    final List<String> ticks = zooKeeper.getChildren("/ticks", selfWatcher());

    for (String tick : ticks) {
      if (!seenTicks.containsKey(Long.valueOf(tick))) {
        final byte[] data = zooKeeper.getData("/ticks/" + tick, false, null);
        final TickInfo tickInfo = serializer.deserialize(data);
        seenTicks.put(Long.valueOf(tick), tickInfo);
        notify.tell(tickInfo, sender());
      }
    }
  }

  private Watcher selfWatcher() {
    return event -> self().tell(event, self());
  }

  private void onEvent(WatchedEvent event) throws KeeperException, InterruptedException {
    if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
      fetchTicks();
    } else {
      LOG().warning("Unexpected event {}", event);
    }
  }
}
