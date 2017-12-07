package com.spbsu.flamestream.runtime.acker;

import com.spbsu.flamestream.core.data.meta.EdgeInstance;

import java.util.HashMap;
import java.util.Map;

public class InMemoryRegistry implements AttachRegistry {
  public final Map<EdgeInstance, Long> registry = new HashMap<>();

  @Override
  public void register(EdgeInstance frontInstance, long attachTimestamp) {
    registry.put(frontInstance, attachTimestamp);

  }
}