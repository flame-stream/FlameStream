package com.spbsu.flamestream.runtime;

import akka.actor.ActorPath;
import akka.actor.ActorPath$;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.serialization.SerializationExtension;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.CommitterConfig;
import com.spbsu.flamestream.runtime.config.ZookeeperWorkersNode;
import com.spbsu.flamestream.runtime.master.ClientWatcher;
import com.spbsu.flamestream.runtime.master.acker.Acker;
import com.spbsu.flamestream.runtime.master.acker.ZkRegistry;
import com.spbsu.flamestream.runtime.serialization.FlameSerializer;
import com.spbsu.flamestream.runtime.serialization.KryoSerializer;
import com.spbsu.flamestream.runtime.state.DevNullStateStorage;
import com.spbsu.flamestream.runtime.state.RocksDBStateStorage;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
import com.spbsu.flamestream.runtime.utils.akka.AwaitResolver;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class StartupWatcher extends LoggingActor {
  private static final RetryPolicy CURATOR_RETRY_POLICY = new ExponentialBackoffRetry(Math.toIntExact(FlameConfig.config
          .smallTimeout()
          .duration()
          .toMillis()), 3);

  private final FlameSerializer kryoSerializer = new KryoSerializer();

  private final String zkConnectString;
  private final String id;
  private final String snapshotPath;
  private final CommitterConfig committerConfig;

  private StateStorage stateStorage = null;
  private CuratorFramework curator = null;

  private StartupWatcher(String id, String zkConnectString, String snapshotPath, CommitterConfig committerConfig) {
    this.zkConnectString = zkConnectString;
    this.id = id;
    this.snapshotPath = snapshotPath;
    this.committerConfig = committerConfig;
  }

  public static Props props(String id, String zkConnectString, String snapshotPath, CommitterConfig committerConfig) {
    return Props.create(StartupWatcher.class, id, zkConnectString, snapshotPath, committerConfig);
  }

  @Override
  public void preStart() throws Exception {
    super.preStart();
    curator = CuratorFrameworkFactory.newClient(zkConnectString, CURATOR_RETRY_POLICY);
    curator.start();
    curator.blockUntilConnected(
            Math.toIntExact(FlameConfig.config.bigTimeout().duration().toMillis()),
            TimeUnit.MILLISECONDS
    );

    if (snapshotPath == null) {
      log().info("No backend is provided, using /dev/null");
      stateStorage = new DevNullStateStorage();
    } else {
      log().info("Initializing rocksDB backend");
      stateStorage = new RocksDBStateStorage(snapshotPath, SerializationExtension.get(context().system()));
    }
    final ZookeeperWorkersNode zookeeperWorkersNode = new ZookeeperWorkersNode(curator, "/workers");
    zookeeperWorkersNode.create(
            id,
            ActorPath$.MODULE$.fromString(self().path()
                    .toStringWithAddress(context().system().provider().getDefaultAddress()))
    );
    ActorRef acker;
    if (zookeeperWorkersNode.isLeader(id)) {
      acker = context().actorOf(Acker.props(committerConfig.defaultMinimalTime(), new ZkRegistry(curator)), "acker");
      context().actorOf(ClientWatcher.props(curator, kryoSerializer, zookeeperWorkersNode), "client-watcher");
    } else {
      acker =
              AwaitResolver.syncResolve(
                      ClusterConfig.fromWorkers(zookeeperWorkersNode.workers())
                              .masterPath()
                              .child("acker"),
                      context()
              );
    }
    context().actorOf(
            ProcessingWatcher.props(
                    id,
                    curator,
                    zookeeperWorkersNode,
                    committerConfig,
                    stateStorage,
                    kryoSerializer,
                    acker
            ),
            "processing-watcher"
    );
    //noinspection ResultOfMethodCallIgnored
    new File("/tmp/flame_stream").createNewFile();
  }

  @Override
  public void postStop() {
    //noinspection EmptyTryBlock,unused
    try (
            StateStorage s = stateStorage;
            CuratorFramework cf = curator
    ) {

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    super.postStop();
  }

  @Override
  public Receive createReceive() {
    return ReceiveBuilder.create()
            .build();
  }
}
