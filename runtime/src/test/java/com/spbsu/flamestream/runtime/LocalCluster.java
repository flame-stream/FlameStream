package com.spbsu.flamestream.runtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.runtime.application.WorkerApplication;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.jooq.lambda.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class LocalCluster implements AutoCloseable {
  private static final String ZK_STRING = "localhost:2181";
  private static final int START_WORKER_PORT = 5223;
  public final Collection<WorkerApplication> workerApplication = new HashSet<>();
  private final Logger LOG = LoggerFactory.getLogger(LocalCluster.class);
  private final ObjectMapper mapper = new ObjectMapper();
  private final ZooKeeper zooKeeper;
  private final ZooKeeperApplication zk;
  private final Thread zkThread;
  private final Map<Integer, InetSocketAddress> dns;

  public LocalCluster(int workersCount) {
    try {
      FileUtils.deleteDirectory(new File("zookeeper"));
      FileUtils.deleteDirectory(new File("leveldb"));

      this.zk = new ZooKeeperApplication();
      this.zkThread = new Thread(Unchecked.runnable(zk::run));
      zkThread.start();
      this.zooKeeper = new ZooKeeper(
              ZK_STRING,
              5000,
              e -> LOG.info("Init zookeeperString ZKEvent: {}", e)
      );

      this.dns = freeSockets(workersCount);

      TimeUnit.SECONDS.sleep(1);
      deployPartitioning();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    workerApplication.addAll(startWorkers());
  }

  public String zookeeperString() {
    return ZK_STRING;
  }

  public Map<Integer, InetSocketAddress> dns() {
    return unmodifiableMap(dns);
  }

  @Override
  public void close() {
    try {
      workerApplication.forEach(WorkerApplication::shutdown);
      workerApplication.clear();

      zk.shutdown();
      zkThread.join();

      FileUtils.deleteDirectory(new File("zookeeper"));
      FileUtils.deleteDirectory(new File("leveldb"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<Integer, InetSocketAddress> freeSockets(int workersCount) {
    return IntStream.range(LocalCluster.START_WORKER_PORT, LocalCluster.START_WORKER_PORT + workersCount)
            .boxed().collect(toMap(Function.identity(),
                    Unchecked.function(port -> new InetSocketAddress(InetAddress.getLocalHost(), port))));
  }

  private void deployPartitioning() {
    try {
      createDirs();
      pushDNS(dns);
      pushFronts(dns.keySet());
    } catch (JsonProcessingException | InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  private void createDirs() throws KeeperException, InterruptedException {
    zooKeeper.create(
            "/ticks",
            new byte[0],
            ZKUtil.parseACLs("world:anyone:crdwa"),
            CreateMode.PERSISTENT
    );
  }

  private void pushDNS(Map<Integer, InetSocketAddress> dns) throws KeeperException, InterruptedException, JsonProcessingException, ZKUtil.BadAclFormatException {
    zooKeeper.create(
            "/dns",
            mapper.writeValueAsBytes(dns),
            ZKUtil.parseACLs("world:anyone:crdwa"),
            CreateMode.PERSISTENT
    );
  }

  private void pushFronts(Set<Integer> fronts) throws KeeperException, InterruptedException, JsonProcessingException, ZKUtil.BadAclFormatException {
    zooKeeper.create(
            "/fronts",
            mapper.writeValueAsBytes(fronts),
            ZKUtil.parseACLs("world:anyone:crdwa"),
            CreateMode.PERSISTENT
    );
  }

  private Collection<WorkerApplication> startWorkers() {
    final Set<WorkerApplication> apps = dns.entrySet().stream()
            .map(f -> new WorkerApplication(f.getKey(), f.getValue(), ZK_STRING))
            .collect(toSet());

    apps.forEach(WorkerApplication::run);
    return apps;
  }
}
