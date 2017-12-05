package com.spbsu.flamestream.runtime.acker;

import java.util.HashMap;
import java.util.Map;

public class InMemoryRegistry implements AttachRegistry {
  public final Map<String, Long> registry = new HashMap<>();

  @Override
  public void register(String frontId, String nodeId, long attachTimestamp) {
    registry.put(frontId + nodeId, attachTimestamp);
  }
}