package com.spbsu.flamestream.core;

import com.spbsu.flamestream.core.data.meta.EdgeId;
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

  abstract class Stub implements Front {
    protected final EdgeId edgeId;
    private long prevGlobalTs = 0;

    protected Stub(EdgeId edgeId) {
      this.edgeId = edgeId;
    }

    protected synchronized GlobalTime currentTime() {
      long globalTs = System.currentTimeMillis();
      if (globalTs <= prevGlobalTs) {
        globalTs = prevGlobalTs + 1;
      }
      prevGlobalTs = globalTs;
      return new GlobalTime(globalTs, edgeId);
    }
  }
}
