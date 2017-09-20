package com.spbsu.datastream.core.node;

import akka.actor.Props;
import com.spbsu.datastream.core.LoggingActor;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;

import static org.apache.zookeeper.Watcher.Event;

public final class LifecycleWatcher extends LoggingActor {
  private final String zkConnectString;
  private final int id;

  private ZooKeeper zk;

  public LifecycleWatcher(String zkConnectString, int id) {
    this.zkConnectString = zkConnectString;
    this.id = id;
  }

  public static Props props(String zkConnectString, int id) {
    return Props.create(LifecycleWatcher.class, zkConnectString, id);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();

    this.zk = new ZooKeeper(zkConnectString, 5000,
            event -> self().tell(event, self()));
  }

  @Override
  public void postStop() throws Exception {
    super.postStop();

    zk.close();
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder().match(WatchedEvent.class, this::onWatchedEvent).build();
  }

  private void onWatchedEvent(WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      final Event.KeeperState state = event.getState();

      switch (state) {
        case SyncConnected:
          LOG().info("Connected to ZK");
          initConcierge();
          break;
        case Expired:
          LOG().info("Session expired");
          context().stop(self());
          break;
        default:
          unhandled(event);
      }
    } else {
      unhandled(event);
    }
  }

  private void initConcierge() {
    context().actorOf(NodeConcierge.props(id, zk), "concierge");
  }
}
