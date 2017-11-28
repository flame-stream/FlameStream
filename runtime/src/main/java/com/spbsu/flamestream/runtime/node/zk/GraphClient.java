package com.spbsu.flamestream.runtime.node.zk;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.spbsu.flamestream.core.Graph;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.util.List;
import java.util.function.Consumer;

public class GraphClient {
  private final Kryo kryo;
  private final ZooKeeper zooKeeper;

  public GraphClient(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
    this.kryo = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  public List<String> graphs(Consumer<List<String>> watcher) {
    try {
      return zooKeeper.getChildren(
              "/graphs",
              event -> watcher.accept(graphs(watcher)),
              null
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Graph<?, ?> graphById(String id) {
    try {
      final byte[] data = zooKeeper.getData("/graphs/" + id, false, null);
      final ByteBufferInput input = new ByteBufferInput(data);
      return kryo.readObject(input, Graph.class);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> fronts(String graphId, String frontId, Consumer<List<String>> watcher) {
    try {
      return zooKeeper.getChildren(
              "/graphs/" + graphId + "/fronts/" + frontId,
              event -> watcher.accept(graphs(watcher)),
              null
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Graph<?, ?> frontById(String graphId, String frontId) {
    try {
      final byte[] data = zooKeeper.getData("/graphs/" + graphId + "/fronts/" + frontId, false, null);
      final ByteBufferInput input = new ByteBufferInput(data);
      return kryo.readObject(input, Graph.class);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void putGraph(String graphId, Graph<?, ?> graph) {
    try {
      final ByteBufferOutput o = new ByteBufferOutput(1000, 20000);
      kryo.writeObject(o, graph);

      zooKeeper.create(
              "/graphs/" + graphId,
              o.toBytes(),
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );

      zooKeeper.create(
              "/graphs/" + graphId + "/fronts",
              new byte[0],
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public void createDirectoryStructure() {
    try {
      zooKeeper.create(
              "/graphs",
              new byte[0],
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
