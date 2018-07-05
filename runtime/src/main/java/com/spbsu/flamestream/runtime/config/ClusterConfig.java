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
  private final ComputationProps props;
  private final int millisBetweenCommits;
  private final int defaultMinTime;

  @JsonCreator
  public ClusterConfig(@JsonProperty("paths") Map<String, ActorPath> paths,
                       @JsonProperty("masterLocation") String masterLocation,
                       @JsonProperty("props") ComputationProps props,
                       @JsonProperty("millisBetweenCommits") int millisBetweenCommits,
                       @JsonProperty("defaultMinTime") int defaultMinTime) {
    this.paths = paths;
    this.masterLocation = masterLocation;
    this.props = props;
    this.millisBetweenCommits = millisBetweenCommits;
    this.defaultMinTime = defaultMinTime;
  }

  @JsonProperty
  public Map<String, ActorPath> paths() {
    return paths;
  }

  @JsonProperty
  public String masterLocation() {
    return masterLocation;
  }

  @JsonIgnore
  public int defaultMinTime() {
    return defaultMinTime;
  }

  @JsonProperty
  public ComputationProps props() {
    return props;
  }

  @JsonProperty
  public int millisBetweenCommits() {
    return this.millisBetweenCommits;
  }

  public ClusterConfig withChildPath(String childPath) {
    final Map<String, ActorPath> newPaths = new HashMap<>();
    paths.forEach((s, path) -> newPaths.put(s, path.child(childPath)));
    return new ClusterConfig(newPaths, masterLocation, props, millisBetweenCommits, 0);
  }

  @Override
  public String toString() {
    return "ClusterConfig{" +
            "paths=" + paths +
            ", masterLocation='" + masterLocation + '\'' +
            ", props=" + props +
            '}';
  }
}
