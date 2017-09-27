package com.spbsu.flamestream.benchmark.config;

import com.spbsu.flamestream.benchmark.EnvironmentRunner;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public interface ClusterRunnerCfg {
  Class<? extends EnvironmentRunner> runner();
}
