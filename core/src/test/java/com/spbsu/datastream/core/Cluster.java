package com.spbsu.datastream.core;

import java.net.InetSocketAddress;
import java.util.Map;

public interface Cluster {
  String zookeeperString();

  Map<Integer, InetSocketAddress> fronts();

  Map<Integer, InetSocketAddress> nodes();
}
