package com.spbsu.flamestream.runtime.acker;

import com.spbsu.flamestream.core.data.meta.EdgeId;

import java.util.HashMap;
import java.util.Map;

public class InMemoryRegistry implements AttachRegistry {
  public final Map<EdgeId, Long> registry = new HashMap<>();

  @Override
  public void register(EdgeId frontId, long attachTimestamp) {
    registry.put(frontId, attachTimestamp);

  }
}