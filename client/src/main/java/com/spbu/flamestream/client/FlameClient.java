package com.spbu.flamestream.client;

import com.spbsu.flamestream.core.Job;

import java.io.Closeable;

public interface FlameClient extends Closeable {
  void push(Job job);

  void close();
}
