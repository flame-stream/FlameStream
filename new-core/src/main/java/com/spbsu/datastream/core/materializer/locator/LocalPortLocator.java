package com.spbsu.datastream.core.materializer.locator;

import com.spbsu.datastream.core.graph.OutPort;
import com.spbsu.datastream.core.materializer.atomic.DataSink;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class LocalPortLocator implements PortLocator {
  private final Map<OutPort, DataSink> storage;

  public LocalPortLocator() {
    this(Collections.emptyMap());
  }

  public LocalPortLocator(final Map<OutPort, DataSink> storage) {
    this.storage = new ConcurrentHashMap<>(storage);
  }

  @Override
  public Optional<DataSink> sinkForPort(final OutPort port) {
    return Optional.ofNullable(storage.get(port));
  }

  public void registerPort(final OutPort port, final DataSink consumer) {
    storage.put(port, consumer);
  }
}
