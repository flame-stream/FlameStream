package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.graph.TheGraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public final class TickInfo {
  private final long id;

  private final TheGraph graph;

  private final Map<HashRange, Integer> hashMapping;

  private final Set<Long> tickDependencies;

  private final long startTs;

  private final long stopTs;

  private final long window;

  private final int ackerLocation;

  public TickInfo(long id,
                  long startTs,
                  long stopTs,
                  TheGraph graph,
                  int ackerLocation,
                  Map<HashRange, Integer> hashMapping,
                  long window,
                  Set<Long> tickDependencies) {
    this.id = id;
    this.ackerLocation = ackerLocation;
    this.hashMapping = new HashMap<>(hashMapping);
    this.graph = graph;
    this.startTs = startTs;
    this.window = window;
    this.stopTs = stopTs;
    this.tickDependencies = new HashSet<>(tickDependencies);
  }

  public long id() {
    return id;
  }

  public long startTs() {
    return startTs;
  }

  public long stopTs() {
    return stopTs;
  }

  public TheGraph graph() {
    return graph;
  }

  public int ackerLocation() {
    return ackerLocation;
  }

  public Map<HashRange, Integer> hashMapping() {
    return unmodifiableMap(hashMapping);
  }

  public long window() {
    return window;
  }

  public Set<Long> tickDependencies() {
    return unmodifiableSet(tickDependencies);
  }

  @Override
  public String toString() {
    return "TickInfo{" +
            "id=" + id +
            ", graph=" + graph +
            ", hashMapping=" + hashMapping +
            ", tickDependencies=" + tickDependencies +
            ", startTs=" + startTs +
            ", stopTs=" + stopTs +
            ", window=" + window +
            ", ackerLocation=" + ackerLocation +
            '}';
  }
}