package com.spbsu.flamestream.core;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

public interface Cluster extends AutoCloseable {
  String zookeeperString();

  Set<Integer> fronts();

  Map<Integer, InetSocketAddress> nodes();
}
