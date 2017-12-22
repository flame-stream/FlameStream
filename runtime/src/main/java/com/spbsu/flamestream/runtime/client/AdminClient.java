package com.spbsu.flamestream.runtime.client;

import com.spbsu.flamestream.runtime.config.ClusterConfig;

public interface AdminClient {
  ClusterConfig config();

  void put(ClusterConfig config);
}