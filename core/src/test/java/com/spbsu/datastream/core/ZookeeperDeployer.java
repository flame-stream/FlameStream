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

final class ZookeeperDeployer implements Closeable {
  private final Logger LOG = LoggerFactory.getLogger(ZookeeperDeployer.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final TickInfoSerializer serializer = new KryoInfoSerializer();
  private final ZooKeeper zooKeeper;

  ZookeeperDeployer(String zkString) throws IOException {
    this.zooKeeper = new ZooKeeper(zkString, 5000, e -> LOG.info("Init zookeeperString ZKEvent: {}", e));
  }

  public void pushDNS(Map<Integer, InetSocketAddress> dns) throws Exception {
    zooKeeper.create("/dns",
            mapper.writeValueAsBytes(dns),
            ZKUtil.parseACLs("world:anyone:crdwa"),
            CreateMode.PERSISTENT);
  }

  public void pushFronts(Set<Integer> fronts) throws Exception {
    zooKeeper.create("/fronts",
            mapper.writeValueAsBytes(fronts),
            ZKUtil.parseACLs("world:anyone:crdwa"),
            CreateMode.PERSISTENT);
  }

  public void pushTick(TickInfo tickInfo) throws Exception {
    zooKeeper.create("/ticks/" + tickInfo.startTs(), serializer.serialize(tickInfo),
            ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
    zooKeeper.create("/ticks/" + tickInfo.startTs() + "/ready", new byte[0],
            ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
    zooKeeper.create("/ticks/" + tickInfo.startTs() + "/committed", new byte[0],
            ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);

  }

  @Override
  public void close() {
    try {
      zooKeeper.close();
    } catch (InterruptedException e) {
      LOG.error("Smth bad happens during closing ZookeeperDeployer", e);
      throw new RuntimeException(e);
    }
  }

  public void createDirs() throws Exception {
    zooKeeper.create("/ticks", new byte[0],
            ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
  }
}
