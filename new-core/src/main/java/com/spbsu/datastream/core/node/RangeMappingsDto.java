package com.spbsu.datastream.core.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.datastream.core.HashRange;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.stream.Collectors;

public class RangeMappingsDto {
  private final Map<HashRange, InetSocketAddress> rangeMappings;

  @JsonCreator
  public RangeMappingsDto(@JsonProperty("mappings") final Map<String, InetSocketAddress> rangeMappings) {
    this(rangeMappings.entrySet().stream().collect(Collectors.toMap(e -> HashRange.fromString(e.getKey()), Map.Entry::getValue)), false);
  }

  private RangeMappingsDto(final Map<HashRange, InetSocketAddress> rangeMappings, final boolean a) {
    this.rangeMappings = rangeMappings;
  }

  public static RangeMappingsDto normalConstruct(final Map<HashRange, InetSocketAddress> rangeMappings) {
    return new RangeMappingsDto(rangeMappings, false);
  }

  @JsonProperty("mappings")
  public Map<String, InetSocketAddress> serialized() {
    return rangeMappings.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
  }

  @Override
  public String toString() {
    return "RangeMappingsDto{" + "rangeMappings=" + rangeMappings +
            '}';
  }

  @JsonIgnore
  public Map<HashRange, InetSocketAddress> rangeMappings() {
    return rangeMappings;
  }
}
