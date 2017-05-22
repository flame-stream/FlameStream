package com.spbsu.datastream.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.datastream.core.configuration.KryoInfoSerializer;
import com.spbsu.datastream.core.configuration.TickInfoSerializer;
import com.spbsu.datastream.core.tick.TickInfo;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

final class ZKDeployer implements Closeable {
  private final Logger LOG = LoggerFactory.getLogger(ZKDeployer.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final TickInfoSerializer serializer = new KryoInfoSerializer();
  private final ZooKeeper zooKeeper;

  ZKDeployer(String zkString) throws IOException {
    this.zooKeeper = new ZooKeeper(zkString, 5000, e -> this.LOG.info("Init zookeeper ZKEvent: {}", e));
  }

  public void pushDNS(Map<Integer, InetSocketAddress> dns) throws Exception {
    this.zooKeeper.create("/dns",
            this.mapper.writeValueAsBytes(dns),
            ZKUtil.parseACLs("world:anyone:crdwa"),
            CreateMode.PERSISTENT);
  }

  public void pushFronts(Set<Integer> fronts) throws Exception {
    this.zooKeeper.create("/fronts",
            this.mapper.writeValueAsBytes(fronts),
            ZKUtil.parseACLs("world:anyone:crdwa"),
            CreateMode.PERSISTENT);
  }

  public void pushTick(TickInfo tickInfo) throws Exception {
    this.zooKeeper.create("/ticks/" + tickInfo.startTs(), this.serializer.serialize(tickInfo),
            ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
    this.zooKeeper.create("/ticks/" + tickInfo.startTs() + "/ready", new byte[0],
            ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
    this.zooKeeper.create("/ticks/" + tickInfo.startTs() + "/committed", new byte[0],
            ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);

  }

  @Override
  public void close() {
    try {
      this.zooKeeper.close();
    } catch (InterruptedException e) {
      this.LOG.error("Smth bad happens during closing ZKDeployer", e);
      throw new RuntimeException(e);
    }
  }

  public void createDirs() throws Exception {
    this.zooKeeper.create("/ticks", new byte[0],
            ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
  }
}
