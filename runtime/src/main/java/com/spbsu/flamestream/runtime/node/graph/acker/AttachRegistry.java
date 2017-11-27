package com.spbsu.flamestream.runtime.node.graph.acker;

public interface AttachRegistry {
  void register(String frontId, long attachTimestamp);
}
