package com.spbsu.flamestream.runtime.config;

import akka.actor.ActorPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class ClusterConfig {
  private final Map<String, ActorPath> paths;
  private final String masterLocation;
  private final Map<String, HashGroup> hashGroups;

  @JsonCreator
  public ClusterConfig(@JsonProperty("paths") Map<String, ActorPath> paths,
                       @JsonProperty("masterLocation") String masterLocation,
                       @JsonProperty("hashGroups") Map<String, HashGroup> hashGroups) {
    this.paths = paths;
    this.masterLocation = masterLocation;
    this.hashGroups = hashGroups;
  }

  @JsonProperty
  public Map<String, ActorPath> paths() {
    return paths;
  }

  @JsonProperty
  public String masterLocation() {
    return masterLocation;
  }

  @JsonProperty
  public Map<String, HashGroup> hashGroups() {
    return hashGroups;
  }

  public ComputationProps props(int maxElementsInGraph) {
    return new ComputationProps(hashGroups, maxElementsInGraph);
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
}
