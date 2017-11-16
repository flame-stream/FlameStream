package com.spbsu.flamestream.runtime.node.tick.range.api;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.InPort;

public class AddressedItem {
  private final DataItem<?> item;
  private final InPort port;

  public AddressedItem(DataItem<?> item, InPort port) {
    this.item = item;
    this.port = port;
  }

  public DataItem<?> item() {
    return item;
  }

  public InPort port() {
    return port;
  }

  @Override
  public String toString() {
    return "AddressedItem{" + "item=" + item + ", port=" + port + '}';
  }
}
