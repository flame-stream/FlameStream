package com.spbsu.flamestream.runtime.config;

import akka.actor.ActorPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class ClusterConfig {
  private final Map<String, ActorPath> paths;
  private final String ackerLocation;
  private final ComputationProps layout;

  @JsonCreator
  public ClusterConfig(Map<String, ActorPath> paths, String ackerLocation, ComputationProps layout) {
    this.paths = paths;
    this.ackerLocation = ackerLocation;
    this.layout = layout;
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
    return layout;
  }
}
