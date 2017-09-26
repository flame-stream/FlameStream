package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.runtime.application.WorkerApplication;
import com.spbsu.flamestream.runtime.application.ZooKeeperApplication;
import org.apache.commons.io.FileUtils;
import org.jooq.lambda.Unchecked;

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
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public final class LocalCluster implements Cluster {
  private static final String ZK_STRING = "localhost:2181";

  private static final int START_WORKER_PORT = 5223;

  private final Map<Integer, InetSocketAddress> dns;
  private final Set<Integer> fronts;

  private final Collection<WorkerApplication> workerApplication = new HashSet<>();
  private final ZooKeeperApplication zk;
  private final Thread zkThread;

  @Override
  public String zookeeperString() {
    return ZK_STRING;
  }

  @Override
  public Set<Integer> fronts() {
    return unmodifiableSet(fronts);
  }

  @Override
  public Map<Integer, InetSocketAddress> nodes() {
    return unmodifiableMap(dns);
  }

  @Override
  public void close() throws Exception {
    workerApplication.forEach(WorkerApplication::shutdown);
    workerApplication.clear();

    zk.shutdown();
    zkThread.join();

    FileUtils.deleteDirectory(new File("zookeeper"));
    FileUtils.deleteDirectory(new File("leveldb"));
  }

  public LocalCluster(int workersCount, int frontCount) {
    try {
      FileUtils.deleteDirectory(new File("zookeeper"));
      FileUtils.deleteDirectory(new File("leveldb"));

      this.dns = freeSockerts(workersCount);
      this.fronts = dns.keySet().stream().limit(frontCount).collect(toSet());

      this.zk = new ZooKeeperApplication();
      this.zkThread = new Thread(Unchecked.runnable(zk::run));
      zkThread.start();

      TimeUnit.SECONDS.sleep(3);
      deployPartitioning();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    workerApplication.addAll(startWorkers());
  }

  private Map<Integer, InetSocketAddress> freeSockerts(int workersCount) {
    return IntStream.range(LocalCluster.START_WORKER_PORT, LocalCluster.START_WORKER_PORT + workersCount)
            .boxed().collect(toMap(Function.identity(),
                    Unchecked.function(port -> new InetSocketAddress(InetAddress.getLocalHost(), port))));
  }

  private void deployPartitioning() throws Exception {
    try (ZookeeperDeployer zkConfigurationDeployer = new ZookeeperDeployer(ZK_STRING)) {
      zkConfigurationDeployer.createDirs();
      zkConfigurationDeployer.pushDNS(dns);
      zkConfigurationDeployer.pushFronts(fronts);
    }
  }

  private Collection<WorkerApplication> startWorkers() {
    final Set<WorkerApplication> apps = dns.entrySet().stream()
            .map(f -> new WorkerApplication(f.getKey(), f.getValue(), ZK_STRING))
            .collect(toSet());

    apps.forEach(WorkerApplication::run);
    return apps;
  }
}
