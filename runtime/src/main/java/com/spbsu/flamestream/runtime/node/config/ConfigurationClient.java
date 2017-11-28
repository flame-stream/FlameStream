package com.spbsu.flamestream.runtime.node.config;

import java.util.function.Consumer;

public interface ConfigurationClient {
  ClusterConfig configuration(Consumer<ClusterConfig> watcher);

  void put(ClusterConfig configuration);
}
