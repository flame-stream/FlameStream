package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.Rear;
import com.spbsu.flamestream.runtime.application.ZooKeeperGraphClient;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.edge.SystemEdgeContext;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Stream;

public class RemoteRuntime implements FlameRuntime {
  private final Logger log = LoggerFactory.getLogger(RemoteRuntime.class);
  private final ZooKeeperGraphClient client;

  public RemoteRuntime(String zkString) throws IOException {
    this.client = new ZooKeeperGraphClient(new ZooKeeper(
            zkString,
            5000,
            (e) -> {
            }
    ));
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  @Override
  public Flame run(Graph g) {
    log.info("Pushing graph {}", g);
    final ZooKeeperGraphClient.ZooKeeperFlameClient graphClient = client.push(g);
    final ClusterConfig config = client.config();
    return new RemoteFlame(config.withChildPath(graphClient.name()), graphClient);
  }

  private class RemoteFlame implements Flame {
    private final ClusterConfig clusterConfig;
    private final ZooKeeperGraphClient.ZooKeeperFlameClient graphClient;

    private RemoteFlame(ClusterConfig config, ZooKeeperGraphClient.ZooKeeperFlameClient graphClient) {
      this.clusterConfig = config;
      this.graphClient = graphClient;
    }

    @Override
    public void close() {
      log.info("Extinguishing graph");
      graphClient.removeGraph();
    }

    @Override
    public <F extends Front, H> Stream<H> attachFront(String id, FrontType<F, H> type) {
      log.info("Attaching front id: '{}', type: '{}'", id, type);
      graphClient.attachFront(id, type.instance());
      return clusterConfig.paths()
              .entrySet()
              .stream()
              .map(e -> type.handle(new SystemEdgeContext(e.getValue(), e.getKey(), id)));
    }

    @Override
    public <R extends Rear, H> Stream<H> attachRear(String id, RearType<R, H> type) {
      log.info("Attaching rear id: '{}', type: '{}'", id, type);
      graphClient.attachRear(id, type.instance());
      return clusterConfig.paths()
              .entrySet()
              .stream()
              .map(e -> type.handle(new SystemEdgeContext(e.getValue(), e.getKey(), id)));
    }
  }
}
