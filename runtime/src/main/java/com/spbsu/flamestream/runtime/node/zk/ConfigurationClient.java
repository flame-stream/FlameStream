package com.spbsu.flamestream.runtime.node.zk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.runtime.node.ClusterConfiguration;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.function.Consumer;

public class ConfigurationClient {
  private final ObjectMapper mapper = new ObjectMapper();
  private final ZooKeeper zooKeeper;

  public ConfigurationClient(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
  }

  public ClusterConfiguration configuration(Consumer<ClusterConfiguration> watcher) {
    try {
      final byte[] data = zooKeeper.getData(
              "/congig",
              event -> watcher.accept(configuration(watcher)),
              null
      );
      return mapper.readValue(data, ClusterConfiguration.class);
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void putConfig(ClusterConfiguration configuration) {
    try {
      final Stat stat = zooKeeper.exists("/config", false);
      zooKeeper.setData(
              "/config",
              mapper.writeValueAsBytes(configuration),
              stat.getVersion()
      );
    } catch (JsonProcessingException | InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }
}
