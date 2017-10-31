package com.spbsu.flamestream.benchmark.config;

import com.spbsu.flamestream.runtime.environment.Environment;

/**
 * User: Artem
 * Date: 18.08.2017
 */
interface EnvironmentProvider {
  Environment environment();
}