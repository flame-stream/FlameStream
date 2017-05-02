package com.spbsu.datastream.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.configuration.RangeMappingsDto;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

public final class ZK implements Closeable {
  private final Logger LOG = LoggerFactory.getLogger(ZK.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final ZooKeeper zooKeeper;

  public ZK(final String zkString) throws IOException {
    this.zooKeeper = new ZooKeeper(zkString, 5000, e -> this.LOG.info("Init zookeeper ZKEvent: {}", e));
  }

  public void pushFrontMappings(
          final Map<Integer, InetSocketAddress> fronts) throws KeeperException, InterruptedException, JsonProcessingException {
    final Stat stat = this.zooKeeper.exists("/fronts", false);
    if (stat != null) {
      this.zooKeeper.delete("/fronts", stat.getVersion());
    }

    this.zooKeeper.create("/fronts", this.mapper.writeValueAsBytes(fronts), ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
  }

  public void pushRangeMappings(
          final Map<HashRange, InetSocketAddress> ranges) throws JsonProcessingException, KeeperException, InterruptedException {
    final Stat stat = this.zooKeeper.exists("/ranges", false);
    if (stat != null) {
      this.zooKeeper.delete("/ranges", stat.getVersion());
    }

    this.zooKeeper.create("/ranges", this.mapper.writeValueAsBytes(RangeMappingsDto.normalConstruct(ranges)), ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
  }

  @Override
  public void close() {
    try {
      this.zooKeeper.close();
    } catch (final InterruptedException e) {
      this.LOG.error("Smth bad happens during closing ZK", e);
      throw new RuntimeException(e);
    }
  }
}
