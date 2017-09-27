package com.spbsu.flamestream.runtime.node;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.ack.CommitTick;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.configuration.KryoInfoSerializer;
import com.spbsu.flamestream.runtime.configuration.TickInfoSerializer;
import com.spbsu.flamestream.runtime.tick.TickCommitDone;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TickWatcher extends LoggingActor {
  private final TickInfoSerializer serializer = new KryoInfoSerializer();
  private final ZooKeeper zooKeeper;
  private final ActorRef subscriber;

  private final Map<Long, TickInfo> seenTicks = new HashMap<>();

  private TickWatcher(ZooKeeper zooKeeper, ActorRef subscriber) {
    this.zooKeeper = zooKeeper;
    this.subscriber = subscriber;
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
    return ReceiveBuilder.create()
            .match(WatchedEvent.class, this::onEvent)
            .match(CommitTick.class, this::commitTick)
            .matchAny(this::unhandled)
            .build();
  }

  private void onEvent(WatchedEvent event) throws KeeperException, InterruptedException {
    if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
      fetchTicks();
    } else if (event.getType() == Watcher.Event.EventType.NodeCreated
            && event.getPath().endsWith("committed")) {
      final long tickId = Long.parseLong(event.getPath().split("/")[2]);
      subscriber.tell(new TickCommitDone(tickId), self());
    } else {
      LOG().info("Unexpected event {}", event);
    }
  }

  private void commitTick(CommitTick commit) throws KeeperException, InterruptedException {
    zooKeeper.create(
            "/ticks/" + commit.tickId() + "/committed",
            new byte[0], ZKUtil.parseACLs("world:anyone:crdwa"),
            CreateMode.PERSISTENT
    );
  }

  private void fetchTicks() throws KeeperException, InterruptedException {
    final List<String> ticks = zooKeeper.getChildren("/ticks", selfWatcher());

    for (String tick : ticks) {
      if (!seenTicks.containsKey(Long.parseLong(tick))) {
        final byte[] data = zooKeeper.getData("/ticks/" + tick, false, null);
        final boolean committed = zooKeeper.exists("/ticks/" + tick + "/committed", selfWatcher()) != null;
        if (!committed) {
          final TickInfo tickInfo = serializer.deserialize(data);
          seenTicks.put(Long.parseLong(tick), tickInfo);
          subscriber.tell(tickInfo, sender());
        } else {
          subscriber.tell(new TickCommitDone(Long.parseLong(tick)), self());
        }
      }
    }
  }

  private Watcher selfWatcher() {
    return event -> self().tell(event, self());
  }
}
