package com.spbsu.flamestream.runtime.acker;

import com.spbsu.flamestream.core.data.meta.EdgeId;

public interface AttachRegistry {
  void register(EdgeId frontId, long attachTimestamp);

  long registeredTime(EdgeId frontId);
}
