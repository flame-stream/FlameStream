package com.spbsu.flamestream.runtime.application;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.node.FlameNode;
import com.spbsu.flamestream.runtime.node.config.ConfigurationClient;
import com.spbsu.flamestream.runtime.node.config.ClusterConfig;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class ZooKeeperFlameClient implements FlameNode.ZookeeperClient, ConfigurationClient {
  private final Kryo kryo;
  private final ObjectMapper mapper = new ObjectMapper();
  private final ZooKeeper zooKeeper;

  public ZooKeeperFlameClient(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    this.kryo = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  @Override
  public Set<String> graphs(Consumer<Set<String>> watcher) {
    try {
      return new HashSet<>(zooKeeper.getChildren(
              "/setObserver",
              event -> watcher.accept(graphs(watcher)),
              null
      ));
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Graph graphBy(String id) {
    try {
      final byte[] data = zooKeeper.getData("/setObserver/" + id, false, null);
      final ByteBufferInput input = new ByteBufferInput(data);
      return kryo.readObject(input, Graph.class);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> fronts(String graphId, String frontId, Consumer<List<String>> watcher) {
    try {
      return zooKeeper.getChildren(
              "/setObserver/" + graphId + "/fronts/" + frontId,
              event -> watcher.accept(fronts(graphId, frontId, watcher)),
              null
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Graph frontById(String graphId, String frontId) {
    try {
      final byte[] data = zooKeeper.getData("/setObserver/" + graphId + "/fronts/" + frontId, false, null);
      final ByteBufferInput input = new ByteBufferInput(data);
      return kryo.readObject(input, Graph.class);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(String graphId, Graph graph) {
    try {
      final ByteBufferOutput o = new ByteBufferOutput(1000, 20000);
      kryo.writeObject(o, graph);

      zooKeeper.create(
              "/setObserver/" + graphId,
              o.toBytes(),
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );

      zooKeeper.create(
              "/setObserver/" + graphId + "/fronts",
              new byte[0],
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ClusterConfig configuration(Consumer<ClusterConfig> watcher) {
    try {
      final byte[] data = zooKeeper.getData(
              "/congig",
              event -> watcher.accept(configuration(watcher)),
              null
      );
      return mapper.readValue(data, ClusterConfig.class);
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(ClusterConfig configuration) {
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

  public void createDirectoryStructure() {
    try {
      zooKeeper.create(
              "/setObserver",
              new byte[0],
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
