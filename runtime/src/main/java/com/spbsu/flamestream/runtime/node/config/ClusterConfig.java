package com.spbsu.flamestream.runtime.node.config;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ClusterConfig {
  private final List<NodeConfig> nodeConfigs;
  private final String ackerLocation;

  @JsonCreator
  public ClusterConfig(@JsonProperty("nodes") List<NodeConfig> nodeConfigs,
                       @JsonProperty("acker_location") String ackerLocation) {
    this.nodeConfigs = new ArrayList<>(nodeConfigs);
    this.ackerLocation = ackerLocation;
  }

  @JsonProperty("nodes")
  private List<NodeConfig> nodeConfigsForSerialization() {
    return nodeConfigs;
  }

  @JsonProperty("acker_location")
  public String ackerLocation() {
    return ackerLocation;
  }

  @JsonIgnore
  public Stream<NodeConfig> nodeConfigs() {
    return nodeConfigs.stream();
  }

  @Override
  public String toString() {
    return "ClusterConfig{" +
            "nodeConfigs=" + nodeConfigs +
            ", ackerLocation='" + ackerLocation + '\'' +
            '}';
  }
}
