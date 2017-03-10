package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandleImpl;
import com.spbsu.datastream.core.materializer.locator.LocalPortLocator;
import com.spbsu.datastream.core.materializer.locator.RemotePortLocator;
import org.apache.zookeeper.ZooKeeper;

public class TickContextImpl implements TickContext {
  private final AtomicHandle handle;

  private final LocalPortLocator localPortLocator;

  private final ZooKeeper zooKeeper;

  private final long tick;

  public TickContextImpl(final ZooKeeper zooKeeper, final long tick) {
    this.tick = tick;
    this.localPortLocator = new LocalPortLocator();
    final RemotePortLocator remotePortLocator = new RemotePortLocator(zooKeeper, tick);
    this.handle = new AtomicHandleImpl(localPortLocator.compose(remotePortLocator));
    this.zooKeeper = zooKeeper;
  }

  @Override
  public long tick() {
    return tick;
  }

  @Override
  public ZooKeeper zookeeper() {
    return zooKeeper;
  }

  @Override
  public LocalPortLocator localLocator() {
    return localPortLocator;
  }

  @Override
  public AtomicHandle handle() {
    return handle;
  }
}
