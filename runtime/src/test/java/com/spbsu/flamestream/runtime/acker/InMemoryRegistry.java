package com.spbsu.flamestream.runtime.acker;

import com.spbsu.flamestream.core.data.meta.EdgeId;

import java.util.HashMap;
import java.util.Map;

public class InMemoryRegistry implements Registry {
  public final Map<EdgeId, Long> registry = new HashMap<>();
  public long lastCommit = 0;

  @Override
  public void register(EdgeId frontId, long attachTimestamp) {
    registry.put(frontId, attachTimestamp);

  }

  @Override
  public long registeredTime(EdgeId frontId) {
    return registry.getOrDefault(frontId, -1L);
  }

  @Override
  public void committed(long time) {
    lastCommit = time;
  }

  @Override
  public long lastCommit() {
    return lastCommit;
  }
}