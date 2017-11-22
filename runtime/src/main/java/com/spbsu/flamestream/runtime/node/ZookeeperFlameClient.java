package com.spbsu.flamestream.runtime.node;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.ComposedGraph;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import org.apache.commons.lang.math.IntRange;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;

public class ZookeeperFlameClient {
  private final ObjectMapper mapper = new ObjectMapper();
  private final ZooKeeper zooKeeper;

  public ZookeeperFlameClient(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;
  }

  public NavigableMap<IntRange, DumbInetSocketAddress> dns() {
    try {
      final byte[] data = zooKeeper.getData("/dns", false, null);
      return dnsFrom(data);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public NavigableMap<IntRange, DumbInetSocketAddress> dns(Consumer<NavigableMap<IntRange, DumbInetSocketAddress>> watcher) {
    try {
      final byte[] data = zooKeeper.getData(
              "/dns",
              event -> watcher.accept(dns(watcher)),
              null
      );
      return dnsFrom(data);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  // FIXME: 11/22/17 Serialize HashRange to Key
  private NavigableMap<IntRange, DumbInetSocketAddress> dnsFrom(byte[] data) {
    try {
      return mapper.readValue(
              data,
              TypeFactory.defaultInstance().constructMapType(TreeMap.class, String.class, DumbInetSocketAddress.class)
      );
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public NavigableMap<String, ComposedGraph<AtomicGraph>> graphs() {
    try {
      final List<String> graphIds = zooKeeper.getChildren(
              "/graphs",
              false,
              null
      );

      final NavigableMap<String, ComposedGraph<AtomicGraph>> instances = new TreeMap<>();
      for (String graphId : graphIds) {
        final byte[] data = zooKeeper.getData("/graphs/" + graphId, false, null);
        instances.put(graphId, graphFrom(data));
      }
      return instances;
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public NavigableMap<String, ComposedGraph<AtomicGraph>> graphs(Consumer<NavigableMap<String, ComposedGraph<AtomicGraph>>> watcher) {
    try {
      final List<String> graphIds = zooKeeper.getChildren(
              "/graphs",
              event -> watcher.accept(graphs(watcher)),
              null
      );

      final NavigableMap<String, ComposedGraph<AtomicGraph>> instances = new TreeMap<>();
      for (String graphId : graphIds) {
        final byte[] data = zooKeeper.getData("/graphs/" + graphId, false, null);
        instances.put(graphId, graphFrom(data));
      }
      return instances;
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private ComposedGraph<AtomicGraph> graphFrom(byte[] data) {
    try {
      return mapper.readValue(
              data,
              TypeFactory.defaultInstance()
                      .constructParametrizedType(ComposedGraph.class, ComposedGraph.class, AtomicGraph.class)
      );
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void putGraph(String graphId, ComposedGraph<AtomicGraph> graph) {
    try {
      zooKeeper.create(
              "/graphs/" + graphId,
              mapper.writeValueAsBytes(graph),
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
    } catch (JsonProcessingException | InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public void putDNS(NavigableMap<String, DumbInetSocketAddress> dns) {
    try {
      final Stat stat = zooKeeper.exists("/dns", false);
      zooKeeper.setData(
              "/dns",
              mapper.writeValueAsBytes(dns),
              stat.getVersion()
      );
    } catch (JsonProcessingException | InterruptedException | KeeperException e) {
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
      zooKeeper.create(
              "/dns",
              mapper.writeValueAsBytes(new TreeMap<>()),
              ZKUtil.parseACLs("world:anyone:cwr"),
              CreateMode.PERSISTENT
      );
    } catch (KeeperException | InterruptedException | JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
