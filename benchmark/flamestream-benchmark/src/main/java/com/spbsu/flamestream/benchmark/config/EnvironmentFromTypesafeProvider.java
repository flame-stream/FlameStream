package com.spbsu.flamestream.benchmark.config;

import com.spbsu.flamestream.runtime.environment.Environment;
import com.spbsu.flamestream.runtime.environment.local.LocalClusterEnvironment;
import com.spbsu.flamestream.runtime.environment.local.LocalEnvironment;
import com.spbsu.flamestream.runtime.environment.remote.RemoteEnvironment;
import com.typesafe.config.Config;

/**
 * User: Artem
 * Date: 18.08.2017
 */
public class EnvironmentFromTypesafeProvider implements EnvironmentProvider {
  private final Environment environment;

  public EnvironmentFromTypesafeProvider(Config load) {
    if (load.hasPath("local-environment")) {
      environment = new LocalEnvironment();
    } else if (load.hasPath("local-cluster-environment")) {
      environment = new LocalClusterEnvironment(load.getInt("local-cluster-environment.workers"));
    } else if (load.hasPath("remote-environment")) {
      environment = new RemoteEnvironment(load.getString("remote-environment.zk"));
    } else {
      throw new RuntimeException("Illegal config");
    }
  }

  @Override
  public Environment environment() {
    return environment;
  }
}
