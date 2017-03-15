package com.spbsu.datastream.core.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.datastream.core.HashRange;

import java.net.InetSocketAddress;
import java.util.Map;

public class RangeMappingsDto {
  private final Map<HashRange, InetSocketAddress> rangeMappings;

  @JsonCreator
  public RangeMappingsDto(@JsonProperty("rangeMappings") final Map<HashRange, InetSocketAddress> rangeMappings) {
    this.rangeMappings = rangeMappings;
  }

  @JsonProperty("rangeMappings")
  public Map<HashRange, InetSocketAddress> rangeMappings() {
    return rangeMappings;
  }
}
