package com.spbsu.flamestream.runtime.environment.local;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.runtime.application.WorkerApplication;
import com.spbsu.flamestream.runtime.environment.Environment;
import com.spbsu.flamestream.runtime.environment.remote.RemoteEnvironment;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.jooq.lambda.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class LocalClusterEnvironment implements Environment {
  private static final String ZK_STRING = "localhost:2181";
  private static final int START_WORKER_PORT = 5223;

  private static final Logger LOG = LoggerFactory.getLogger(LocalClusterEnvironment.class);
  private final ObjectMapper mapper = new ObjectMapper();

  private final Collection<WorkerApplication> workerApplication = new HashSet<>();
  private final Map<Integer, InetSocketAddress> dns;

  private final ZooKeeper zooKeeper;
  private final ZooKeeperApplication zk;
  private final Thread zkThread;

  private final RemoteEnvironment remoteEnvironment;

  public LocalClusterEnvironment(int workersCount) {
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
      TimeUnit.SECONDS.sleep(3);

      deployPartitioning();
      this.remoteEnvironment = new RemoteEnvironment(ZK_STRING);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    workerApplication.addAll(startWorkers());
  }

  @Override
  public void close() {
    try {
      remoteEnvironment.close();

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
    return IntStream.range(LocalClusterEnvironment.START_WORKER_PORT, LocalClusterEnvironment.START_WORKER_PORT + workersCount)
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

  @Override
  public void deploy(TickInfo tickInfo) {
    remoteEnvironment.deploy(tickInfo);
  }

  @Override
  public Set<Integer> availableFronts() {
    return remoteEnvironment.availableFronts();
  }

  @Override
  public Set<Integer> availableWorkers() {
    return remoteEnvironment.availableWorkers();
  }

  @Override
  public <T> AtomicGraph wrapInSink(ToIntFunction<? super T> hash, Consumer<? super T> mySuperConsumer) {
    return remoteEnvironment.wrapInSink(hash, mySuperConsumer);
  }

  @Override
  public Consumer<Object> frontConsumer(int frontId) {
    return remoteEnvironment.frontConsumer(frontId);
  }

  private static final class ZooKeeperApplication extends ZooKeeperServerMain {
    void run() throws IOException {
      final QuorumPeerConfig quorumConfig = new QuorumPeerConfig();

      try {
        final Properties props = new Properties();
        props.setProperty("clientPort", "2181");
        props.setProperty("tickTime", "2000");
        props.setProperty("dataDir", "./zookeeper");
        quorumConfig.parseProperties(props);
      } catch (QuorumPeerConfig.ConfigException | IOException e) {
        throw new RuntimeException(e);
      }

      final ServerConfig serverConfig = new ServerConfig();
      serverConfig.readFrom(quorumConfig);

      runFromConfig(serverConfig);
    }

    @Override
    public void shutdown() {
      super.shutdown();
    }
  }
}
