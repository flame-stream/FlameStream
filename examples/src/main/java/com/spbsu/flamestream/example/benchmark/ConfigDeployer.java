package com.spbsu.flamestream.example.benchmark;

import akka.actor.ActorPath;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.runtime.application.ZooKeeperGraphClient;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.config.ConfigurationClient;
import com.spbsu.flamestream.runtime.config.HashRange;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigDeployer {
  private final ConfigDeployerConfig config;

  public ConfigDeployer(ConfigDeployerConfig config) {
    this.config = config;
  }

  public static void main(String... args) throws IOException {
    try (final BufferedReader br = Files.newBufferedReader(Paths.get(args[0]))) {
      final ConfigDeployerConfig configDeployerConfig = new ObjectMapper().readValue(br, ConfigDeployerConfig.class);
      new ConfigDeployer(configDeployerConfig).run();
    }
  }

  private void run() throws IOException {
    final Map<String, ActorPath> paths = new HashMap<>();
    config.workers.forEach((s, address) -> {
      final Address a = new Address("akka", "worker", address.host(), address.port());
      final ActorPath path = RootActorPath.apply(a, "/").child("user").child("watcher");
      paths.put(s, path);
    });

    final Map<String, HashRange> ranges = new HashMap<>();
    final List<HashRange> covering = HashRange.covering(paths.size() - 1)
            .collect(Collectors.toCollection(ArrayList::new));
    paths.keySet().forEach(s -> {
      if (s.equals(config.ackerLocation)) {
        ranges.put(s, new HashRange(0, 0));
      } else {
        ranges.put(s, covering.get(0));
        covering.remove(0);
      }
    });
    assert covering.isEmpty();

    final ComputationProps props = new ComputationProps(ranges, config.maxElementsInGraph);
    final ClusterConfig clusterConfig = new ClusterConfig(paths, config.ackerLocation, props);

    final ConfigurationClient client = new ZooKeeperGraphClient(new ZooKeeper(config.zkString, 4000, event -> {}));
    client.put(clusterConfig);
  }

  public static class ConfigDeployerConfig {
    private final String zkString;
    private final Map<String, DumbInetSocketAddress> workers;
    private final String ackerLocation;
    private final int maxElementsInGraph;

    public ConfigDeployerConfig(@JsonProperty("zkString") String zkString,
                                @JsonProperty("workers") Map<String, DumbInetSocketAddress> workers,
                                @JsonProperty("ackerLocation") String ackerLocation,
                                @JsonProperty("maxElementsInGraph") int maxElementsInGraph) {
      this.zkString = zkString;
      this.workers = workers;
      this.ackerLocation = ackerLocation;
      this.maxElementsInGraph = maxElementsInGraph;
    }
  }
}
