package com.spbu.flamestream.client;

import com.spbsu.flamestream.core.Job;

public interface FlameClient {
  void push(Job job);
}
