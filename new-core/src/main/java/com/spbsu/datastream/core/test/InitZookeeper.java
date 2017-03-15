package com.spbsu.datastream.core.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.application.ZooKeeperApplication;
import com.spbsu.datastream.core.node.RangeMappingsDto;
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
import java.util.concurrent.TimeUnit;

public class InitZookeeper {
  public static void main(final String... args) throws Exception {
    new InitZookeeper().run();
  }

  private void run() throws IOException, KeeperException, InterruptedException {
    initZookeeper();
  }

  private void initZookeeper() throws IOException, KeeperException, InterruptedException {
    final ZooKeeper zooKeeper = new ZooKeeper("localhost:2181", 5000, System.out::println);
    final Stat stat = zooKeeper.exists("/mappings", false);
    if (stat != null) {
      zooKeeper.delete("/mappings", stat.getVersion());
    }

    zooKeeper.create("/mappings", mappings(), ZKUtil.parseACLs("world:anyone:crdwa"), CreateMode.PERSISTENT);
    zooKeeper.close();
  }

  private byte[] mappings() throws JsonProcessingException {
    final Map<HashRange, InetSocketAddress> mappings = new HashMap<>();

    final InetSocketAddress myAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7001);
    mappings.putIfAbsent(new HashRange(Integer.MIN_VALUE, Integer.MAX_VALUE), myAddress);
    //mappings.putIfAbsent(new HashRange(Integer.MIN_VALUE, 1000), myAddress);
    //mappings.putIfAbsent(new HashRange(1000, Integer.MAX_VALUE), myAddress);
    ObjectMapper mapper = new ObjectMapper();

    System.err.println(mapper.writeValueAsString(RangeMappingsDto.normalConstruct(mappings)));

    return mapper.writeValueAsBytes(RangeMappingsDto.normalConstruct(mappings));
  }
}
