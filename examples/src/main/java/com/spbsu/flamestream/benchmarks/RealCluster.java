package com.spbsu.flamestream.benchmarks;

import com.spbsu.flamestream.core.Cluster;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public final class RealCluster implements Cluster {
  private final String zookeeperString;
  private final Map<Integer, InetSocketAddress> nodes;
  private final Set<Integer> fronts;

  public RealCluster(String zookeeperString,
                     Map<Integer, InetSocketAddress> nodes,
                     Set<Integer> fronts) {
    this.zookeeperString = zookeeperString;
    this.nodes = new HashMap<>(nodes);
    this.fronts = new HashSet<>(fronts);
  }

  @Override
  public String zookeeperString() {
    return zookeeperString;
  }

  @Override
  public Set<Integer> fronts() {
    return unmodifiableSet(fronts);
  }

  @Override
  public Map<Integer, InetSocketAddress> nodes() {
    return unmodifiableMap(nodes);
  }

  @Override
  public void close() throws Exception {
  }
}
