package com.spbsu.flamestream.runtime;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.serialization.SerializationExtension;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.master.ClientWatcher;
import com.spbsu.flamestream.runtime.serialization.FlameSerializer;
import com.spbsu.flamestream.runtime.serialization.JacksonSerializer;
import com.spbsu.flamestream.runtime.serialization.KryoSerializer;
import com.spbsu.flamestream.runtime.state.DevNullStateStorage;
import com.spbsu.flamestream.runtime.state.RocksDBStateStorage;
import com.spbsu.flamestream.runtime.state.StateStorage;
import com.spbsu.flamestream.runtime.utils.FlameConfig;
import com.spbsu.flamestream.runtime.utils.akka.LoggingActor;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.TimeUnit;

public class StartupWatcher extends LoggingActor {
  private static final RetryPolicy CURATOR_RETRY_POLICY = new ExponentialBackoffRetry(Math.toIntExact(FlameConfig.config
          .smallTimeout()
          .duration()
          .toMillis()), 3);

  private final FlameSerializer kryoSerializer = new KryoSerializer();
  private final FlameSerializer jacksonSerializer = new JacksonSerializer();

  private final String zkConnectString;
  private final String id;
  private final String snapshotPath;

  private StateStorage stateStorage = null;
  private ClusterConfig config = null;
  private CuratorFramework curator = null;
  private NodeCache configCache = null;

  private StartupWatcher(String id, String zkConnectString, String snapshotPath) {
    this.zkConnectString = zkConnectString;
    this.id = id;
    this.snapshotPath = snapshotPath;
  }

  public static Props props(String id, String zkConnectString, String snapshotPath) {
    return Props.create(StartupWatcher.class, id, zkConnectString, snapshotPath);
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

    configCache = new NodeCache(curator, "/config");
    configCache.getListenable().addListener(() -> {
      final ClusterConfig clusterConfig = jacksonSerializer.deserialize(
              configCache.getCurrentData().getData(),
              ClusterConfig.class
      );
      self().tell(clusterConfig, self());
    });
    configCache.start();

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
    //noinspection EmptyTryBlock,unused
    try (
            StateStorage s = stateStorage;
            NodeCache cc = configCache;
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
            .match(ClusterConfig.class, this::onConfig)
            .build();
  }

  private void onConfig(ClusterConfig config) {
    if (this.config != null) {
      throw new RuntimeException("Config updating is not supported yet");
    }

    this.config = config;
    if (id.equals(config.masterLocation())) {
      context().actorOf(ClientWatcher.props(curator, kryoSerializer, config), "client-watcher");
    }
    context().actorOf(ProcessingWatcher.props(id, curator, config, stateStorage, kryoSerializer), "processing-watcher");
  }
}
