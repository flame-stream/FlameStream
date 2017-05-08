package com.spbsu.datastream.core.tick;

import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class TickInfo {
  private final TheGraph graph;

  private final Map<HashRange, Integer> hashMapping;

  private final long startTs;

  private final long stopTs;

  private final long window;

  private final int ackerLocation;

  public TickInfo(final TheGraph graph,
                  final int ackerLocation,
                  final Map<HashRange, Integer> hashMapping,
                  final long startTs,
                  final long stopTs,
                  final long window) {
    this.ackerLocation = ackerLocation;
    this.hashMapping = new HashMap<>(hashMapping);
    this.graph = graph;
    this.startTs = startTs;
    this.window = window;
    this.stopTs = stopTs;
  }

  public Map<HashRange, Integer> hashMapping() {
    return Collections.unmodifiableMap(this.hashMapping);
  }

  public long stopTs() {
    return this.stopTs;
  }

  public int ackerLocation() {
    return this.ackerLocation;
  }

  public long window() {
    return this.window;
  }

  public TheGraph graph() {
    return this.graph;
  }

  public long startTs() {
    return this.startTs;
  }

  @Override
  public String toString() {
    return "TickInfo{" + "graph=" + this.graph +
            ", hashMapping=" + this.hashMapping +
            ", startTs=" + this.startTs +
            ", stopTs=" + this.stopTs +
            ", window=" + this.window +
            ", ackerLocation=" + this.ackerLocation +
            '}';
  }
}