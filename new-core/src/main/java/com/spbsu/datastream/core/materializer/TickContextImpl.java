package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandleImpl;
import com.spbsu.datastream.core.materializer.locator.LocalPortLocator;
import com.spbsu.datastream.core.materializer.locator.RemotePortLocator;
import org.apache.zookeeper.ZooKeeper;

public class TickContextImpl implements TickContext {
  private final AtomicHandle handle;

  private final RemotePortLocator remotePortLocator;
  private final LocalPortLocator localPortLocator;

  private final ZooKeeper zooKeeper;

  public TickContextImpl(final ZooKeeper zooKeeper, final long tick) {
    this.localPortLocator = new LocalPortLocator();
    this.remotePortLocator = new RemotePortLocator(zooKeeper, tick);
    this.handle = new AtomicHandleImpl(localPortLocator.compose(remotePortLocator));
    this.zooKeeper = zooKeeper;
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
