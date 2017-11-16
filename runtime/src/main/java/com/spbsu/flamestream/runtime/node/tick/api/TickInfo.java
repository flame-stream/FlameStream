package com.spbsu.flamestream.runtime.node.tick.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.ComposedGraph;
import com.spbsu.flamestream.runtime.node.tick.range.HashRange;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public class TickInfo {
  private final long id;

  private final ComposedGraph<AtomicGraph> graph;

  private final Map<HashRange, String> hashMapping;

  private final Set<String> fronts;

  private final Set<Long> tickDependencies;

  private final long startTs;

  private final long stopTs;

  private final long window;

  private final String ackerLocation;

  public TickInfo(long id,
          long startTs,
          long stopTs,
          ComposedGraph<AtomicGraph> graph,
          String ackerLocation,
          Map<HashRange, String> hashMapping,
          Set<String> fronts,
          long window,
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

  public String ackerLocation() {
    return ackerLocation;
  }

  public Map<HashRange, String> hashMapping() {
    return unmodifiableMap(hashMapping);
  }

  public Set<String> fronts() {
    return unmodifiableSet(fronts);
  }

  public long window() {
    return window;
  }

  public Set<Long> tickDependencies() {
    return unmodifiableSet(tickDependencies);
  }

  public boolean isInTick(GlobalTime time) {
    return time.time() >= startTs && time.time() < stopTs;
  }

  @Override
  public String toString() {
    return "TickInfo{" + "id=" + id + ", graph=" + graph + ", hashMapping=" + hashMapping + ", tickDependencies="
            + tickDependencies + ", startTs=" + startTs + ", stopTs=" + stopTs + ", window=" + window
            + ", ackerLocation=" + ackerLocation + '}';
  }
}