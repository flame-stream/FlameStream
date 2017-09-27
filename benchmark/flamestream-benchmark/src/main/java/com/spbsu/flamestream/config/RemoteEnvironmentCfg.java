package com.spbsu.flamestream.config;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class RemoteEnvironmentCfg {
  private final String zkString;
  private final Map<Integer, InetSocketAddress> nodes;
  private final Set<Integer> fronts;

  RemoteEnvironmentCfg(String zkString, Map<Integer, InetSocketAddress> nodes, Set<Integer> fronts) {
    this.zkString = zkString;
    this.nodes = nodes;
    this.fronts = fronts;
  }

  public String zkString() {
    return zkString;
  }

  public Set<Integer> fronts() {
    return fronts;
  }

  public Map<Integer, InetSocketAddress> nodes() {
    return nodes;
  }
}
