package com.spbsu.datastream.core.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public final class RangeMappingsDto {
  private final Map<HashRange, InetSocketAddress> rangeMappings;

  @JsonCreator
  public RangeMappingsDto(@JsonProperty("mappings") final Map<String, InetSocketAddress> rangeMappings) {
    this(rangeMappings.entrySet().stream().collect(Collectors.toMap(e -> HashRange.fromString(e.getKey()), Map.Entry::getValue)), false);
  }

  public static RangeMappingsDto normalConstruct(final Map<HashRange, InetSocketAddress> rangeMappings) {
    return new RangeMappingsDto(rangeMappings, false);
  }

  @SuppressWarnings("SameParameterValue")
  @JsonIgnore
  private RangeMappingsDto(final Map<HashRange, InetSocketAddress> rangeMappings,
                           final boolean variableForDifferentSignature) {
    this.rangeMappings = rangeMappings;
  }

  @JsonProperty("mappings")
  public Map<String, InetSocketAddress> serialized() {
    return this.rangeMappings.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
  }

  @Override
  public String toString() {
    return "RangeMappingsDto{" + "rangeMappings=" + this.rangeMappings +
            '}';
  }

  @JsonIgnore
  public Map<HashRange, InetSocketAddress> rangeMappings() {
    return Collections.unmodifiableMap(this.rangeMappings);
  }
}
