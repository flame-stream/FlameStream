package com.spbsu.flamestream.runtime.acker;

import com.spbsu.flamestream.core.data.meta.EdgeInstance;

public interface AttachRegistry {
  void register(EdgeInstance frontInstance, long attachTimestamp);
}
