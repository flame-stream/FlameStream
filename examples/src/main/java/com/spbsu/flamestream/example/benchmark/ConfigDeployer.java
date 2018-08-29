package com.spbsu.flamestream.example.benchmark;

import akka.actor.ActorPath;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ComputationProps;
import com.spbsu.flamestream.runtime.config.HashGroup;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.serialization.JacksonSerializer;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigDeployer {
  private final ConfigDeployerConfig config;

  private ConfigDeployer(ConfigDeployerConfig config) {
    this.config = config;
  }

  public static void main(String... args) throws IOException {
    try (final BufferedReader br = Files.newBufferedReader(Paths.get(args[0]))) {
      final ConfigDeployerConfig configDeployerConfig = new ObjectMapper().readValue(br, ConfigDeployerConfig.class);
      new ConfigDeployer(configDeployerConfig).run();
    }
  }

  private void run() {
    final Map<String, ActorPath> paths = new HashMap<>();
    config.workers.forEach((s, address) -> {
      final Address a = new Address("akka", "worker", address.host(), address.port());
      final ActorPath path = RootActorPath.apply(a, "/").child("user").child("watcher");
      paths.put(s, path);
    });

    final Map<String, HashGroup> ranges = new HashMap<>();
    final List<HashUnit> covering = HashUnit.covering(paths.size() - 1)
            .collect(Collectors.toCollection(ArrayList::new));
    paths.keySet().forEach(s -> {
      if (s.equals(config.masterLocation)) {
        ranges.put(s, new HashGroup(Collections.singleton(new HashUnit(0, 0))));
      } else {
        ranges.put(s, new HashGroup(Collections.singleton(covering.get(0))));
        covering.remove(0);
      }
    });
    assert covering.isEmpty();

    final ComputationProps props = new ComputationProps(ranges, config.maxElementsInGraph);
    final ClusterConfig clusterConfig = new ClusterConfig(
            paths,
            config.masterLocation,
            props,
            config.millisBetweenCommits,
            0
    );

    final CuratorFramework curator = CuratorFrameworkFactory.newClient(
            config.zkString,
            new ExponentialBackoffRetry(1000, 3)
    );
    curator.start();
    try {
      curator.create().orSetData().forPath("/config", new JacksonSerializer().serialize(clusterConfig));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static class ConfigDeployerConfig {
    private final String zkString;
    private final Map<String, DumbInetSocketAddress> workers;
    private final String masterLocation;
    private final int maxElementsInGraph;
    private final int millisBetweenCommits;

    public ConfigDeployerConfig(@JsonProperty("zkString") String zkString,
                                @JsonProperty("workers") Map<String, DumbInetSocketAddress> workers,
                                @JsonProperty("masterLocation") String masterLocation,
                                @JsonProperty("maxElementsInGraph") int maxElementsInGraph,
                                @JsonProperty("millisBetweenCommits") int millisBetweenCommits) {
      this.zkString = zkString;
      this.workers = workers;
      this.masterLocation = masterLocation;
      this.maxElementsInGraph = maxElementsInGraph;
      this.millisBetweenCommits = millisBetweenCommits;
    }
  }
}
