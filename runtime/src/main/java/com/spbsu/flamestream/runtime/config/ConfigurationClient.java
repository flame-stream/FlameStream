package com.spbsu.flamestream.runtime.config;

import java.util.function.IntConsumer;

public interface ConfigurationClient {
  int epoch(IntConsumer epochWatcher);

  void setEpoch(int epoch);

  ClusterConfig config();

  void put(ClusterConfig config);
}