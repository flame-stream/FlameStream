package com.spbsu.flamestream.runtime.config;

public interface ConfigurationClient {
  ClusterConfig config();

  void put(ClusterConfig config);
}