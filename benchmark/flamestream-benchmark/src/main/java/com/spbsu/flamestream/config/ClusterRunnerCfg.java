package com.spbsu.flamestream.config;

import com.spbsu.flamestream.EnvironmentRunner;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public interface ClusterRunnerCfg {
  Class<? extends EnvironmentRunner> runner();
}
