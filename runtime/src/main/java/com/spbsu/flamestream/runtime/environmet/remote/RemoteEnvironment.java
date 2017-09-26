package com.spbsu.flamestream.runtime.environmet.remote;

import com.spbsu.flamestream.core.TickInfo;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.runtime.environmet.Environment;
import org.apache.zookeeper.ZooDefs;

import java.util.Set;
import java.util.function.Consumer;

public final class RemoteEnvironment implements Environment {
  private final ZookeeperDeployer deployer;
  

  @Override
  public void deploy(TickInfo tickInfo) {
  }

  @Override
  public Set<Integer> availableFronts() {
    return null;
  }

  @Override
  public AtomicGraph wrapInSink(Consumer<Object> mySuperConsumer) {
    return null;
  }

  @Override
  public Consumer<Object> frontConsumer(int frontId) {
    return null;
  }

  @Override
  public void close() throws Exception {

  }
}
