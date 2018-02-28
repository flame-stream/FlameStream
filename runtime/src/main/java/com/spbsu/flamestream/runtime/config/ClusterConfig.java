package com.spbsu.flamestream.runtime.config;

import akka.actor.ActorPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

public class ClusterConfig {
  private final Map<String, ActorPath> paths;
  private final String ackerLocation;
  private final ComputationProps props;

  @JsonCreator
  public ClusterConfig(@JsonProperty("paths") Map<String, ActorPath> paths,
                       @JsonProperty("ackerLocation") String ackerLocation,
                       @JsonProperty("props") ComputationProps props) {
    this.paths = paths;
    this.ackerLocation = ackerLocation;
    this.props = props;
  }

  @JsonProperty
  public Map<String, ActorPath> paths() {
    return paths;
  }

  @JsonProperty
  public String ackerLocation() {
    return ackerLocation;
  }

  @JsonProperty
  public ComputationProps props() {
    return props;
  }


  public ClusterConfig withChildPath(String childPath) {
    final Map<String, ActorPath> newPaths = new HashMap<>();
    paths.forEach((s, path) -> {
      newPaths.put(s, path.child(childPath));
    });
    return new ClusterConfig(newPaths, ackerLocation, props);
  }

  @Override
  public String toString() {
    return "ClusterConfig{" +
            "paths=" + paths +
            ", ackerLocation='" + ackerLocation + '\'' +
            ", props=" + props +
            '}';
  }
}
