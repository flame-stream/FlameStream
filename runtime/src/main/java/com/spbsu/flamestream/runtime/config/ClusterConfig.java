package com.spbsu.flamestream.runtime.config;

import akka.actor.ActorPath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterConfig {
  private final Map<String, ActorPath> paths;
  private final String masterLocation;
  private final Map<String, HashGroup> hashGroups;

  public ClusterConfig(Map<String, ActorPath> paths, String masterLocation, Map<String, HashGroup> hashGroups) {
    this.paths = paths;
    this.masterLocation = masterLocation;
    this.hashGroups = hashGroups;
  }

  public Map<String, ActorPath> paths() {
    return new HashMap<>(paths);
  }

  public String masterLocation() {
    return masterLocation;
  }

  public Map<String, HashGroup> hashGroups() {
    return new HashMap<>(hashGroups);
  }

  public ClusterConfig withChildPath(String childPath) {
    final Map<String, ActorPath> newPaths = new HashMap<>();
    paths.forEach((s, path) -> newPaths.put(s, path.child(childPath)));
    return new ClusterConfig(newPaths, masterLocation, hashGroups);
  }

  @Override
  public String toString() {
    return "ClusterConfig{" +
            "paths=" + paths +
            ", masterLocation='" + masterLocation + '\'' +
            ", hashGroups=" + hashGroups +
            '}';
  }

  public static ClusterConfig fromWorkers(List<ZookeeperWorkersNode.Worker> workers) {
    final Map<String, ActorPath> paths = workers.stream()
            .collect(Collectors.toMap(ZookeeperWorkersNode.Worker::id, ZookeeperWorkersNode.Worker::actorPath));
    final Map<String, HashGroup> ranges = new HashMap<>();
    final List<HashUnit> covering = HashUnit.covering(paths.size() - 1)
            .collect(Collectors.toCollection(ArrayList::new));
    final String masterLocation = workers.get(0).id;
    paths.keySet().forEach(s -> {
      if (s.equals(masterLocation)) {
        ranges.put(s, new HashGroup(Collections.singleton(new HashUnit(0, 0))));
      } else {
        ranges.put(s, new HashGroup(Collections.singleton(covering.get(0))));
        covering.remove(0);
      }
    });
    assert covering.isEmpty();

    return new ClusterConfig(paths, masterLocation, ranges);
  }
}
