package com.spbsu.flamestream.core;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface Rear<H extends Rear.Handle> extends Consumer<Object> {
  H handle();

  interface Handle {
    void detach();
  }
}
