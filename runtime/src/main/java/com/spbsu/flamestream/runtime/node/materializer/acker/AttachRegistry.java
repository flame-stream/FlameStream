package com.spbsu.flamestream.runtime.node.materializer.acker;

public interface AttachRegistry {
  void register(String frontId, long attachTimestamp);
}
