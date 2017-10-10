package com.spbsu.flamestream.benchmark;

import com.spbsu.flamestream.runtime.environment.Environment;
import com.typesafe.config.Config;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public interface EnvironmentRunner {
  void run(Environment environment, Config config);
}
