package com.spbsu.datastream.core.tick;

import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public final class TickInfo {
  private final TheGraph graph;

  private final Map<HashRange, Integer> hashMapping;

  private final long startTs;

  private final long stopTs;

  private final long window;

  private final int ackerLocation;

  public TickInfo(TheGraph graph,
                  int ackerLocation,
                  Map<HashRange, Integer> hashMapping,
                  long startTs,
                  long stopTs,
                  long window) {
    this.ackerLocation = ackerLocation;
    this.hashMapping = new HashMap<>(hashMapping);
    this.graph = graph;
    this.startTs = startTs;
    this.window = window;
    this.stopTs = stopTs;
  }

  public Map<HashRange, Integer> hashMapping() {
    return unmodifiableMap(hashMapping);
  }

  public long stopTs() {
    return stopTs;
  }

  int ackerLocation() {
    return ackerLocation;
  }

  public long window() {
    return window;
  }

  public TheGraph graph() {
    return graph;
  }

  public long startTs() {
    return startTs;
  }

  @Override
  public String toString() {
    return "TickInfo{" + "graph=" + graph +
            ", hashMapping=" + hashMapping +
            ", startTs=" + startTs +
            ", stopTs=" + stopTs +
            ", window=" + window +
            ", ackerLocation=" + ackerLocation +
            '}';
  }
}