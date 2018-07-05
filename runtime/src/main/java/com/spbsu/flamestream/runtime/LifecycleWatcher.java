package com.spbsu.flamestream.runtime;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.serialization.SerializationExtension;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.master.ClientWatcher;
import com.spbsu.flamestream.runtime.zk.ZooKeeperInnerClient;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import com.spbsu.flamestream.runtime.state.DevNullStateStorage;
import com.spbsu.flamestream.runtime.state.RocksDBStateStorage;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;

import java.util.Set;

import static org.apache.zookeeper.Watcher.Event;

public class LifecycleWatcher extends LoggingActor {
  private static final int SESSION_TIMEOUT = 5000;
  private final String zkConnectString;
  private final String id;
  private final String snapshotPath;

  private int epoch;

  private StateStorage stateStorage = null;
  private ZooKeeperInnerClient client = null;

  private LifecycleWatcher(String id, String zkConnectString, String snapshotPath) {
    this.zkConnectString = zkConnectString;
    this.id = id;
    this.snapshotPath = snapshotPath;
  }

  public static Props props(String id, String zkConnectString, String snapshotPath) {
    return Props.create(LifecycleWatcher.class, id, zkConnectString, snapshotPath);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    client = new ZooKeeperInnerClient(new ZooKeeper(
            zkConnectString,
            SESSION_TIMEOUT,
            event -> self().tell(event, self())
    ));
    epoch = client.epoch(newEpoch -> self().tell(newEpoch, self()));
    if (snapshotPath == null) {
      log().info("No backend is provided, using /dev/null");
      stateStorage = new DevNullStateStorage();
    } else {
      log().info("Initializing rocksDB backend");
      stateStorage = new RocksDBStateStorage(snapshotPath, SerializationExtension.get(context().system()));
    }
  }

  @Override
  public void postStop() {
    super.postStop();
    try {
      stateStorage.close();
      client.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .match(WatchedEvent.class, this::onWatchedEvent)
            .match(Integer.class, newEpoch -> {
              if (epoch != -1 && newEpoch != epoch) {
                log().warning("There is new epoch '{}', restarting", epoch);
                // To skip shutdown hooks
                Runtime.getRuntime().halt(12);
              } else {
                epoch = newEpoch;
                log().warning("There is new epoch appeared");
              }
            })
            .build();
  }

  private void initMaster(ClusterConfig config) {
    if (id.equals(config.masterLocation())) {
      context().actorOf(ClientWatcher.props(zkConnectString), "client-watcher");
    }
  }

  private void initGraph(ClusterConfig config) {
    final Graph g = client.graph();
    log().info("Creating node with watchGraphs: '{}', config: '{}'", g, config);


    final ActorRef node = context().actorOf(
            FlameNode.props(
                    id,
                    g,
                    config.withChildPath("graph_at_" + epoch),
                    client,
                    stateStorage
            ),
            "graph_at_" + epoch
    );

    final Set<AttachFront<?>> initialFronts = client.fronts(newFronts ->
            newFronts.forEach(front -> node.tell(front, self()))
    );
    initialFronts.forEach(f -> node.tell(f, self()));

    final Set<AttachRear<?>> initialRears = client.rears(newRears ->
            newRears.forEach(rear -> node.tell(rear, self()))
    );
    initialRears.forEach(r -> node.tell(r, self()));
  }

  private void onWatchedEvent(WatchedEvent event) {
    if (event.getType() == Event.EventType.None) {
      final Event.KeeperState state = event.getState();

      switch (state) {
        case SyncConnected:
          log().info("Connected to ZK");
          client.watchConfig(config -> {
            initMaster(config);
            client.watchGraph(created -> initGraph(config));
          });
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
