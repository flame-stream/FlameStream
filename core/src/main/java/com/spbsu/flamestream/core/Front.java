package com.spbsu.flamestream.core;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 15.11.2017
 */
public interface Front {
  void onStart(Consumer<?> consumer);

  void onRequestNext(long from);

  void onCheckpoint(long to);
}
