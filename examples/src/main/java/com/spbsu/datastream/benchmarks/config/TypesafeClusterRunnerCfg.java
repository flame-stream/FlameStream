package com.spbsu.datastream.benchmarks.config;

import com.spbsu.datastream.benchmarks.ClusterRunner;
import com.typesafe.config.Config;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class TypesafeClusterRunnerCfg implements ClusterRunnerCfg {
  private final Class<? extends ClusterRunner> runner;

  public TypesafeClusterRunnerCfg(Config load) {
    final Config config = load.getConfig("cluster-runner");
    try {
      //noinspection unchecked
      this.runner = ((Class<? extends ClusterRunner>) Class.forName(config.getString("runner")));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Runner is not specified correctly in config");
    }
  }

  @Override
  public Class<? extends ClusterRunner> runner() {
    return runner;
  }
}
