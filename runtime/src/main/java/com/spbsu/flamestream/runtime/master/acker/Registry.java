package com.spbsu.flamestream.runtime.master.acker;

import com.spbsu.flamestream.core.data.meta.EdgeId;

public interface Registry {
  void register(EdgeId frontId, long attachTimestamp);

  long registeredTime(EdgeId frontId);

  void committed(long time);

  long lastCommit();
}
