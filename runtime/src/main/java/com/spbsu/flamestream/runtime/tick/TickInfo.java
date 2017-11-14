package com.spbsu.flamestream.runtime.tick;

import com.spbsu.flamestream.core.graph.atomic.AtomicGraph;
import com.spbsu.flamestream.core.graph.composed.ComposedGraph;
import com.spbsu.flamestream.runtime.range.HashRange;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public final class TickInfo {
  private final long id;

  private final ComposedGraph<AtomicGraph> graph;

  private final Map<HashRange, Integer> hashMapping;

  private final Set<Integer> fronts;

  private final Set<Long> tickDependencies;

  private final long startTs;

  private final long stopTs;

  private final long window;

  private final int ackerLocation;

  public TickInfo(long id,
                  long startTs,
                  long stopTs,
                  ComposedGraph<AtomicGraph> graph,
                  int ackerLocation,
                  Map<HashRange, Integer> hashMapping,
                  Set<Integer> fronts, long window,
                  Set<Long> tickDependencies) {
    if (!graph.inPorts().isEmpty() || !graph.outPorts().isEmpty()) {
      throw new IllegalArgumentException("InPorts or OutPorts are not empty");
    }

    this.id = id;
    this.ackerLocation = ackerLocation;
    this.hashMapping = new HashMap<>(hashMapping);
    this.graph = graph;
    this.startTs = startTs;
    this.fronts = fronts;
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

  public ComposedGraph<AtomicGraph> graph() {
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
    return "TickInfo{" + "id=" + id + ", graph=" + graph + ", hashMapping=" + hashMapping + ", tickDependencies="
        + tickDependencies + ", startTs=" + startTs + ", stopTs=" + stopTs + ", window=" + window
        + ", ackerLocation=" + ackerLocation + '}';
  }

  public Set<Integer> fronts() {
    return fronts;
  }
}