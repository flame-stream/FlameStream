package com.spbsu.flamestream.runtime.node.config;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ClusterConfig {
  private final Map<String, NodeConfig> nodeConfigs;
  private final String ackerLocation;

  @JsonCreator
  public ClusterConfig(@JsonProperty("nodes") Map<String, NodeConfig> nodeConfigs,
                       @JsonProperty("acker_location") String ackerLocation) {
    this.nodeConfigs = new HashMap<>(nodeConfigs);
    this.ackerLocation = ackerLocation;
  }

  @JsonProperty("nodes")
  public Map<String, NodeConfig> nodeConfigs() {
    return nodeConfigs;
  }

  @JsonProperty("acker_location")
  public String ackerLocation() {
    return ackerLocation;
  }


  @Override
  public String toString() {
    return "ClusterConfig{" +
            "nodeConfigs=" + nodeConfigs +
            ", ackerLocation='" + ackerLocation + '\'' +
            '}';
  }
}
