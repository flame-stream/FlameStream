package com.spbsu.datastream.benchmarks.config;

import com.spbsu.datastream.benchmarks.ClusterRunner;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public interface ClusterRunnerCfg {
  Class<? extends ClusterRunner> runner();
}
