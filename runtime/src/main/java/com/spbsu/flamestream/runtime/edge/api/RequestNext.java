package com.spbsu.flamestream.runtime.edge.api;

import com.spbsu.flamestream.core.data.meta.GlobalTime;

public class RequestNext {
  private final GlobalTime time;

  public RequestNext(GlobalTime time) {
    this.time = time;
  }

  public GlobalTime time() {
    return time;
  }
}
