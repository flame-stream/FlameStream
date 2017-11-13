package com.spbsu.flamestream.runtime.source.api;

import com.spbsu.flamestream.core.data.DataItem;

/**
 * User: Artem
 * Date: 10.11.2017
 */
public class Accepted {
  private final DataItem dataItem;

  public Accepted(DataItem dataItem) {
    this.dataItem = dataItem;
  }

  public DataItem dataItem() {
    return this.dataItem;
  }
}
