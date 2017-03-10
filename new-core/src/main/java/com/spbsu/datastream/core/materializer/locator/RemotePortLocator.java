package com.spbsu.datastream.core.materializer.locator;

import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.materializer.atomic.DataSink;
import com.spbsu.datastream.core.materializer.locator.PortLocator;
import org.apache.zookeeper.ZooKeeper;

import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;

public class RemotePortLocator implements PortLocator {
  private final ZooKeeper keeper;

  private final Map<OutPort, DataSink> cache;

  private final long tick;

  public RemotePortLocator(final ZooKeeper keeper, final long tick) {
    this.keeper = keeper;
    this.cache = new WeakHashMap<>();
    this.tick = tick;
  }

  @Override
  public Optional<DataSink> sinkForPort(final OutPort port) {
    return Optional.ofNullable(cache.computeIfAbsent(port, this::remoteConsumer));
  }

  @org.jetbrains.annotations.Nullable
  private DataSink remoteConsumer(final OutPort port) {
    /* TODO:
     * * Go to ZK
     * * Get the nodeId for port
     * * Get the Actor for tick manager
     * * Wrap the actor, so the messages will be marked with InPort
     * * Return the Consumer
     */
    throw new UnsupportedOperationException();
  }
}
