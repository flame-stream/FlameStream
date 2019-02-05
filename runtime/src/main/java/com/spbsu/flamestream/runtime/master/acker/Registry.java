package com.spbsu.flamestream.runtime.master.acker;

import com.spbsu.flamestream.core.data.meta.EdgeId;

import java.util.Map;

public interface Registry {
  Map<EdgeId, Long> all();

  void register(EdgeId frontId, long attachTimestamp);

  long registeredTime(EdgeId frontId);

  void committed(long time);

  long lastCommit();
}
