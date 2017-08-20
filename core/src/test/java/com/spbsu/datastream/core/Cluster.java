package com.spbsu.datastream.core;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

public interface Cluster {
  String zookeeperString();

  Set<Integer> fronts();

  Map<Integer, InetSocketAddress> nodes();
}