package com.spbsu.flamestream.runtime.edge.socket;

import com.spbsu.flamestream.core.Front;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class SocketFront implements Front {
  
  public SocketFront(String host, int port) {
  }

  @Override
  public void onStart(Consumer<Object> consumer, GlobalTime from) {

  }

  @Override
  public void onRequestNext() {
    //socket front does not support backpressure
  }

  @Override
  public void onCheckpoint(GlobalTime to) {
    //socket front does not support checkpoints
  }
}
