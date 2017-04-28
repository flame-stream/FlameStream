package com.spbsu.datastream.core.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.configuration.RangeMappingsDto;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public final class InitZookeeper {
  public static void main(final String... args) throws IOException, KeeperException, InterruptedException {
    new InitZookeeper().run();
  }

  public void run() throws IOException, KeeperException, InterruptedException {
    final ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 5000, System.out::println);

    this.pushRangeMappings(zooKeeper);
    this.pushFrontMappings(zooKeeper);

    zooKeeper.close();
  }

  private void pushFrontMappings(final ZooKeeper zk) throws KeeperException, InterruptedException, JsonProcessingException {
    final Stat stat = zk.exists("/fronts", false);
    if (stat != null) {
      zk.delete("/fronts", stat.getVersion());
    }

    zk.create("/fronts", this.frontMappings(), ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
  }

  private void pushRangeMappings(final ZooKeeper zk) throws InterruptedException, KeeperException, JsonProcessingException {
    final Stat stat = zk.exists("/ranges", false);
    if (stat != null) {
      zk.delete("/ranges", stat.getVersion());
    }

    zk.create("/ranges", this.rangeMappings(), ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
  }

  private byte[] frontMappings() throws JsonProcessingException {
    final Map<String, InetSocketAddress> mappings = new HashMap<>();

    final InetSocketAddress worker1 = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7001);
    mappings.putIfAbsent("1", worker1);

    final InetSocketAddress worker2 = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7002);
    mappings.putIfAbsent("2", worker2);
    final ObjectMapper mapper = new ObjectMapper();

    System.err.println(mapper.writeValueAsString(mappings));

    return mapper.writeValueAsBytes(mappings);

  }

  private byte[] rangeMappings() throws JsonProcessingException {
    final Map<HashRange, InetSocketAddress> mappings = new HashMap<>();

    final InetSocketAddress worker1 = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7001);
    mappings.putIfAbsent(new HashRange(Integer.MIN_VALUE, 0), worker1);

    final InetSocketAddress worker2 = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7002);
    mappings.putIfAbsent(new HashRange(0, Integer.MAX_VALUE), worker2);
    final ObjectMapper mapper = new ObjectMapper();

    System.err.println(mapper.writeValueAsString(RangeMappingsDto.normalConstruct(mappings)));

    return mapper.writeValueAsBytes(RangeMappingsDto.normalConstruct(mappings));
  }
}
