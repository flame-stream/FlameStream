package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.runtime.environment.Environment;
import com.spbsu.flamestream.runtime.environment.remote.RemoteEnvironment;

public final class RemoteTestStand extends AbstractTestStand {
  private final LocalCluster cluster;
  private final RemoteEnvironment environment;

  public RemoteTestStand(int workerCount) {
    this.cluster = new LocalCluster(workerCount);
    this.environment = new RemoteEnvironment(cluster.zookeeperString());
  }

  @Override
  public Environment environment() {
    return environment;
  }

  @Override
  public void close() {
    environment.close();
    cluster.close();
  }
}
