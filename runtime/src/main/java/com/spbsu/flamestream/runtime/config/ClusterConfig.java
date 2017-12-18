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
  public ClusterConfig(@JsonProperty("paths") Map<String, ActorPath> paths,
                       @JsonProperty("acker_location") String ackerLocation,
                       @JsonProperty("ranges") ComputationProps layout) {
    this.paths = paths;
    this.ackerLocation = ackerLocation;
    this.layout = layout;
  }

  @JsonProperty("paths")
  public Map<String, ActorPath> paths() {
    return paths;
  }

  @JsonProperty("acker_location")
  public String ackerLocation() {
    return ackerLocation;
  }

  @JsonProperty("ranges")
  public ComputationProps layout() {
    return layout;
  }
}
