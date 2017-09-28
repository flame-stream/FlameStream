package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.runtime.environment.Environment;
import com.spbsu.flamestream.runtime.environment.local.LocalEnvironment;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public class LocalTestStand extends AbstractTestStand {
  private final LocalEnvironment environment = new LocalEnvironment();

  @Override
  public Environment environment() {
    return environment;
  }

  @Override
  public void close() {
    environment.close();
  }
}
