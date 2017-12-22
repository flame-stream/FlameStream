package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 15.11.2017
 */
public interface Front {
  void onStart(Consumer<Object> consumer, GlobalTime from);

  void onRequestNext();

  void onCheckpoint(GlobalTime to);
}
