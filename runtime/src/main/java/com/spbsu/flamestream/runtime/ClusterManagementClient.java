package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.runtime.config.ClusterConfig;

public interface ClusterManagementClient {
  ClusterConfig config();

  void put(ClusterConfig config);
}