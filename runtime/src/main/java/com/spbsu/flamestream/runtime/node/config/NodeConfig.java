package com.spbsu.flamestream.runtime.node.config;

import akka.actor.ActorPath;
import akka.actor.Address;
import com.sun.javafx.sg.prism.NodePath;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class NodeConfig {
  private final ActorPath nodePath;
  private final HashRange range;

  @JsonCreator
  public NodeConfig(@JsonProperty("node_path") ActorPath nodePath,
                    @JsonProperty("range") HashRange range) {
    this.nodePath = nodePath;
    this.range = range;
  }

  @JsonProperty("node_path")
  public ActorPath nodePath() {
    return nodePath;
  }

  @JsonProperty("range")
  public HashRange range() {
    return range;
  }

  @Override
  public String toString() {
    return "NodeConfig{" +
            "nodePath=" + nodePath +
            ", range=" + range +
            '}';
  }
}
