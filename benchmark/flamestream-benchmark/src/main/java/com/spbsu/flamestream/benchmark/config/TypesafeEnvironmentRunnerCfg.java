package com.spbsu.flamestream.benchmark.config;

import com.spbsu.flamestream.benchmark.EnvironmentRunner;
import com.typesafe.config.Config;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class TypesafeEnvironmentRunnerCfg implements EnvironmentRunnerCfg {
  private final Class<? extends EnvironmentRunner> runner;

  public TypesafeEnvironmentRunnerCfg(Config load) {
    final Config config = load.getConfig("environment-runner");
    try {
      //noinspection unchecked
      this.runner = ((Class<? extends EnvironmentRunner>) Class.forName(config.getString("runner")));
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Runner is not specified correctly in config");
    }
  }

  @Override
  public Class<? extends EnvironmentRunner> runner() {
    return runner;
  }
}
