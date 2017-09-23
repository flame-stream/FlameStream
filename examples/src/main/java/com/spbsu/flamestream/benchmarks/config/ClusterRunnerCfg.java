package com.spbsu.flamestream.benchmarks.config;

import com.spbsu.flamestream.benchmarks.ClusterRunner;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public interface ClusterRunnerCfg {
  Class<? extends ClusterRunner> runner();
}
