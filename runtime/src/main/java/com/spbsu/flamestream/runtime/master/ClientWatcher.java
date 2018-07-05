package com.spbsu.flamestream.runtime.master;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.Job;
import com.spbsu.flamestream.runtime.RemoteRuntime;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import com.spbsu.flamestream.runtime.zk.ZooKeeperExternalClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ClientWatcher extends LoggingActor {
  private static final int SESSION_TIMEOUT = 5000;
  private final String zkString;

  private ZooKeeperExternalClient externalClient = null;
  private RemoteRuntime remoteRuntime = null;

  private ClientWatcher(String zkConnectString) {
    this.zkString = zkConnectString;
  }

  public static Props props(String zkConnectString) {
    return Props.create(ClientWatcher.class, zkConnectString);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    externalClient = new ZooKeeperExternalClient(new ZooKeeper(
            zkString,
            SESSION_TIMEOUT,
            event -> self().tell(event, self())
    ));
    remoteRuntime = new RemoteRuntime(zkString);
  }

  @Override
  public void postStop() {
    try {
      externalClient.close();
      remoteRuntime.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(WatchedEvent.class, this::onWatchedEvent)
            .match(Job.class, this::onNewJob)
            .build();
  }

  private void onNewJob(Job job) {
    final RemoteRuntime.Flame flame = remoteRuntime.run(job.graph());
    job.fronts()
            .forEach(front -> flame.attachFront(
                    front.id(),
                    new SocketFrontType(front.host(), front.port(), front.inputClasses().toArray(Class[]::new))
            ));
    job.rears()
            .forEach(rear -> flame.attachRear(
                    rear.id(),
                    new SocketRearType(rear.host(), rear.port(), rear.outputClasses().toArray(Class[]::new))
            ));
  }

  private void onWatchedEvent(WatchedEvent event) {
    if (event.getType() == Watcher.Event.EventType.None) {
      final Watcher.Event.KeeperState state = event.getState();
      switch (state) {
        case SyncConnected:
          log().info("Connected to ZK");
          externalClient.watchJob(job -> self().tell(job, self()));
          break;
        case Expired:
          log().info("Session expired");
          context().stop(self());
          break;
        case Disconnected:
          log().info("Disconnected");
          break;
        default:
          unhandled(event);
      }
    } else {
      unhandled(event);
    }
  }
}
