package com.spbsu.flamestream.runtime.node;

import com.spbsu.flamestream.runtime.node.config.ClusterConfig;

import java.util.function.Consumer;

public interface ConfigurationClient {
  ClusterConfig configuration(Consumer<ClusterConfig> watcher);

  void put(ClusterConfig configuration);
}
