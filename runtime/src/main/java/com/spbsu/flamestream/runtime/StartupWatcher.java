package com.spbsu.flamestream.runtime;

import akka.actor.ActorPath$;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.serialization.SerializationExtension;
import com.spbsu.flamestream.runtime.config.SystemConfig;
import com.spbsu.flamestream.runtime.config.ZookeeperWorkersNode;
import com.spbsu.flamestream.runtime.master.ClientWatcher;
import com.spbsu.flamestream.runtime.serialization.FlameSerializer;
import com.spbsu.flamestream.runtime.serialization.KryoSerializer;
import com.spbsu.flamestream.runtime.state.DevNullStateStorage;
import com.spbsu.flamestream.runtime.state.RocksDBStateStorage;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
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
  private final SystemConfig systemConfig;

  private StateStorage stateStorage = null;
  private CuratorFramework curator = null;

  private StartupWatcher(String id, String zkConnectString, String snapshotPath, SystemConfig systemConfig) {
    this.zkConnectString = zkConnectString;
    this.id = id;
    this.snapshotPath = snapshotPath;
    this.systemConfig = systemConfig;
  }

  public static Props props(String id, String zkConnectString, String snapshotPath, SystemConfig systemConfig) {
    return Props.create(StartupWatcher.class, id, zkConnectString, snapshotPath, systemConfig);
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
    if (zookeeperWorkersNode.isLeader(id)) {
      context().actorOf(ClientWatcher.props(curator, kryoSerializer, zookeeperWorkersNode), "client-watcher");
    }
    context().actorOf(
            ProcessingWatcher.props(
                    id,
                    curator,
                    zookeeperWorkersNode,
                    systemConfig,
                    stateStorage,
                    kryoSerializer
            ),
            "processing-watcher"
    );
    final File file = new File("/tmp/flame_stream");
    //noinspection ResultOfMethodCallIgnored
    file.createNewFile();
    file.deleteOnExit();
  }

  @Override
  public void postStop() throws Exception {
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
