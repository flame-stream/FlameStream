package com.spbsu.flamestream.runtime.config;

import akka.actor.ActorPath;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

public class NodeConfig {
  private final String id;
  private final ActorPath nodePath;
  private final HashRange range;

  @JsonCreator
  public NodeConfig(@JsonProperty("id") String id,
                    @JsonProperty("node_path") ActorPath nodePath,
                    @JsonProperty("range") HashRange range) {
    this.id = id;
    this.nodePath = nodePath;
    this.range = range;
  }

  @JsonProperty("id")
  public String id() {
    return this.id;
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
            "id='" + id + '\'' +
            ", nodePath=" + nodePath +
            ", range=" + range +
            '}';
  }
}
