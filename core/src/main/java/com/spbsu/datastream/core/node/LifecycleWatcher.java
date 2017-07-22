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

    this.zk = new ZooKeeper(this.zkConnectString, 5000,
            event -> this.self().tell(event, this.self()));
  }

  @Override
  public void postStop() throws Exception {
    super.postStop();

    this.zk.close();
  }

  @Override
  public Receive createReceive() {
    return this.receiveBuilder().match(WatchedEvent.class, this::onWatchedEvent).build();
  }

  private void onWatchedEvent(WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      final Event.KeeperState state = event.getState();

      switch (state) {
        case SyncConnected:
          this.LOG().info("Connected to ZK");
          this.initConcierge();
          break;
        case Expired:
          this.LOG().info("Session expired");
          this.context().stop(this.self());
          break;
        default:
          this.unhandled(event);
      }
    } else {
      this.unhandled(event);
    }
  }

  private void initConcierge() {
    this.context().actorOf(NodeConcierge.props(this.id, this.zk), String.valueOf(this.id));
  }
}
