package com.spbsu.flamestream.runtime.front;

/**
 * User: Artem
 * Date: 15.11.2017
 */
public interface Front {
  void onStart(FrontHandle handle);

  void onRequestNext();

  void onCheckpoint();
}
