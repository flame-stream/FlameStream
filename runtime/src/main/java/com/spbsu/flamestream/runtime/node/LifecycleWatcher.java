package com.spbsu.flamestream.runtime.node;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;

import static org.apache.zookeeper.Watcher.Event;

public class LifecycleWatcher extends LoggingActor {
  public static final int SESSION_TIMEOUT = 5000;
  private final String zkConnectString;
  private final String id;

  private ZooKeeper zk = null;

  private LifecycleWatcher(String zkConnectString, String id) {
    this.zkConnectString = zkConnectString;
    this.id = id;
  }

  public static Props props(String zkConnectString, String id) {
    return Props.create(LifecycleWatcher.class, zkConnectString, id);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();

    this.zk = new ZooKeeper(zkConnectString, SESSION_TIMEOUT, event -> self().tell(event, self()));
  }

  @Override
  public void postStop() {
    super.postStop();

    try {
      zk.close();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(WatchedEvent.class, this::onWatchedEvent)
            .build();
  }

  private void onWatchedEvent(WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      final Event.KeeperState state = event.getState();

      switch (state) {
        case SyncConnected:
          log().info("Connected to ZK");
          context().actorOf(FlameNode.props(id, zk), "concierge");
          break;
        case Expired:
          log().info("Session expired");
          context().stop(self());
          break;
        default:
          unhandled(event);
      }
    } else {
      unhandled(event);
    }
  }
}
