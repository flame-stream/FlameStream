package com.spbsu.flamestream.runtime;

import akka.actor.ActorPath;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.runtime.application.WorkerApplication;
import com.spbsu.flamestream.runtime.application.ZooKeeperGraphClient;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ConfigurationClient;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.config.HashGroup;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZooKeeperApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LocalClusterRuntime implements FlameRuntime {
  private final Logger log = LoggerFactory.getLogger(LocalClusterRuntime.class);
  private final ZooKeeperApplication zooKeeperApplication;
  private final RemoteRuntime remoteRuntime;
  private final Set<WorkerApplication> workers = new HashSet<>();

  private LocalClusterRuntime(int parallelism, int maxElementsInGraph, int millisBetweenCommits) throws IOException {
    final List<Integer> ports = new ArrayList<>(freePorts(parallelism + 1));

    this.zooKeeperApplication = new ZooKeeperApplication(ports.get(0));
    zooKeeperApplication.run();

    final String zkString = "localhost:" + ports.get(0);
    final ConfigurationClient configClient = new ZooKeeperGraphClient(new ZooKeeper(
            zkString,
            1000,
            (w) -> {
            }
    ));

    final Map<String, ActorPath> workersAddresses = new HashMap<>();
    for (int i = 0; i < parallelism; i++) {
      final String name = "worker" + i;
      final DumbInetSocketAddress address = new DumbInetSocketAddress("localhost", ports.get(i + 1));
      final WorkerApplication worker = new WorkerApplication(name, address, zkString);
      final ActorPath path = RootActorPath.apply(Address.apply(
              "akka",
              "worker",
              address.host(),
              address.port()
      ), "/").child("user").child("watcher");
      workersAddresses.put(name, path);
      workers.add(worker);
      worker.run();
    }

    final ClusterConfig config = config(workersAddresses, maxElementsInGraph, millisBetweenCommits);
    log.info("Pushing configuration {}", config);
    configClient.put(config);
    this.remoteRuntime = new RemoteRuntime(zkString);
  }

  @Override
  public void close() {
    workers.forEach(WorkerApplication::close);
    zooKeeperApplication.shutdown();
  }

  @Override
  public Flame run(Graph g) {
    return remoteRuntime.run(g);
  }

  private ClusterConfig config(Map<String, ActorPath> workers, int maxElementsInGraph, int millisBetweenCommits) {
    final String ackerLocation = workers.keySet().stream().findAny().orElseThrow(IllegalArgumentException::new);

    final Map<String, HashGroup> rangeMap = new HashMap<>();
    final List<HashUnit> ranges = HashUnit.covering(workers.size()).collect(Collectors.toList());
    workers.keySet().forEach(name -> {
      rangeMap.put(name, new HashGroup(Collections.singleton(ranges.get(0))));
      ranges.remove(0);
    });
    assert ranges.isEmpty();

    final ComputationProps computationProps = new ComputationProps(rangeMap, maxElementsInGraph);
    return new ClusterConfig(workers, ackerLocation, computationProps, millisBetweenCommits, 0);
  }

  private Set<Integer> freePorts(int n) throws IOException {
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

  public static class Builder {
    private int parallelism = DEFAULT_PARALLELISM;
    private int maxElementsInGraph = DEFAULT_MAX_ELEMENTS_IN_GRAPH;
    private int millisBetweenCommits = DEFAULT_MILLIS_BETWEEN_COMMITS;

    public Builder parallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder maxElementsInGraph(int maxElementsInGraph) {
      this.maxElementsInGraph = maxElementsInGraph;
      return this;
    }

    public Builder millisBetweenCommits(int millisBetweenCommits) {
      this.millisBetweenCommits = millisBetweenCommits;
      return this;
    }

    public LocalClusterRuntime build() throws IOException {
      return new LocalClusterRuntime(parallelism, maxElementsInGraph, millisBetweenCommits);
    }
  }
}
