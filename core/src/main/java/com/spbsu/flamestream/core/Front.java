package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 15.11.2017
 */
public interface Front<H extends Front.Handle> {
  void onStart(Consumer<?> consumer);

  void onRequestNext(GlobalTime from);

  void onCheckpoint(GlobalTime to);

  H handle();
  
  interface Handle {
    void detach();
  }
}
