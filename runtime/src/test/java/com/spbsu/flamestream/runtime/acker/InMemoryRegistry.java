package com.spbsu.flamestream.runtime.acker;

import com.spbsu.flamestream.core.data.meta.EdgeId;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

public class InMemoryRegistry implements AttachRegistry {
  public final Map<EdgeId, Long> registry = new HashMap<>();

  @Override
  public void register(EdgeId frontId, long attachTimestamp) {
    registry.put(frontId, attachTimestamp);

  }

  @Override
  public long registeredTime(EdgeId frontId) {
    return registry.getOrDefault(frontId, -1L);
  }
}