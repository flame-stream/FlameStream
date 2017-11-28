package com.spbsu.flamestream.runtime.node.acker;

public interface AttachRegistry {
  void register(String frontId, long attachTimestamp);
}
