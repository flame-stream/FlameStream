package com.spbsu.flamestream.runtime.acker;

public interface AttachRegistry {
  void register(String frontId, String nodeId, long attachTimestamp);
}
