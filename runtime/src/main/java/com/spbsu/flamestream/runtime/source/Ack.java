package com.spbsu.flamestream.runtime.source;

import com.spbsu.flamestream.core.data.DataItem;

public final class Ack {
  private final DataItem<?> dataItem;

  public Ack(DataItem<?> dataItem) {
    this.dataItem = dataItem;
  }

  public DataItem<?> dataItem() {
    return dataItem;
  }
}
