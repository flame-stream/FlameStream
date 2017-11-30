package com.spbsu.flamestream.runtime.config;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;

public class ClusterConfig {
  @JsonProperty("node_configs")
  private final Collection<NodeConfig> nodeConfigs;
  @JsonProperty("acker_location")
  private final String ackerLocation;

  @JsonCreator
  public ClusterConfig(@JsonProperty("node_configs") Collection<NodeConfig> nodeConfigs,
                       @JsonProperty("acker_location") String ackerLocation) {
    this.nodeConfigs = new ArrayList<>(nodeConfigs);
    this.ackerLocation = ackerLocation;
  }

  @JsonIgnore
  public Stream<NodeConfig> nodes() {
    return nodeConfigs.stream();
  }

  @JsonIgnore
  public NodeConfig ackerNode() {
    return nodeConfigs.stream()
            .filter(nodeConfig -> nodeConfig.id().equals(ackerLocation))
            .findFirst().orElseThrow(() -> new IllegalStateException("Acker location is invalid"));
  }

  @Override
  public String toString() {
    return "ClusterConfig{" +
            "nodeConfigs=" + nodeConfigs +
            ", ackerLocation='" + ackerLocation + '\'' +
            '}';
  }
}
