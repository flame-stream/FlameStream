package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;
import com.spbsu.datastream.core.materializer.locator.LocalPortLocator;
import org.apache.zookeeper.ZooKeeper;

public interface TickContext {
  long tick();

  ZooKeeper zookeeper();

  LocalPortLocator localLocator();

  AtomicHandle handle();
}
