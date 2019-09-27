package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ZookeeperWorkersNode;
import com.spbsu.flamestream.runtime.serialization.KryoSerializer;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.server.ZooKeeperApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class LocalClusterRuntime extends RemoteRuntime {
  private static final Logger LOG = LoggerFactory.getLogger(LocalClusterRuntime.class);

  private final ZooKeeperApplication zooKeeperApplication;
  private final Set<WorkerApplication> workers;
  private final String zkString;
  private final CuratorFramework curator;

  public interface WorkerConfigFactory {
    WorkerApplication.WorkerConfig create(String name, DumbInetSocketAddress localAddress, String zkString);
  }

  public static LocalClusterRuntime create(int parallelism, WorkerConfigFactory workerConfigFactory) {
    final List<Integer> ports;
    try {
      ports = new ArrayList<>(freePorts(parallelism + 1));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final ZooKeeperApplication zooKeeperApplication = new ZooKeeperApplication(ports.get(0));
    zooKeeperApplication.run();

    final String zkString = "localhost:" + ports.get(0);
    LOG.info("ZK string: {}", zkString);

    final Set<WorkerApplication> workers = new HashSet<>();
    for (int i = 0; i < parallelism; i++) {
      final String name = "worker" + i;
      final DumbInetSocketAddress address = new DumbInetSocketAddress("localhost", ports.get(i + 1));

      final WorkerApplication worker = new WorkerApplication(workerConfigFactory.create(name, address, zkString));
      workers.add(worker);
      worker.run();
    }

    final CuratorFramework curator = CuratorFrameworkFactory.newClient(
            zkString,
            new ExponentialBackoffRetry(1000, 3)
    );
    curator.start();
    final ZookeeperWorkersNode workersNode = new ZookeeperWorkersNode(curator, "/workers");
    while (workersNode.workers().size() != parallelism) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
    }

    final ClusterConfig config = ClusterConfig.fromWorkers(workersNode.workers());
    LOG.info("Pushing configuration {}", config);
    return new LocalClusterRuntime(zooKeeperApplication, workers, zkString, curator, config);
  }

  public LocalClusterRuntime(
          ZooKeeperApplication zooKeeperApplication,
          Set<WorkerApplication> workers,
          String zkString,
          CuratorFramework curator,
          ClusterConfig config
  ) {
    super(curator, new KryoSerializer(), config);
    this.zooKeeperApplication = zooKeeperApplication;
    this.workers = workers;
    this.zkString = zkString;
    this.curator = curator;
  }

  @Override
  public void close() {
    try {
      super.close();
      curator.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      workers.forEach(WorkerApplication::close);
      zooKeeperApplication.shutdown();
    }
  }

  public String zkString() {
    return zkString;
  }

  private static Set<Integer> freePorts(int n) throws IOException {
    final Set<ServerSocket> sockets = new HashSet<>();
    final Set<Integer> ports = new HashSet<>();
    try {
      for (int i = 0; i < n; ++i) {
        final ServerSocket socket = new ServerSocket(0);
        ports.add(socket.getLocalPort());
        sockets.add(socket);
      }
    } finally {
      for (ServerSocket socket : sockets) {
        socket.close();
      }
    }
    return ports;
  }
}
