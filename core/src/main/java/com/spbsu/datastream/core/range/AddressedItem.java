package com.spbsu.datastream.core.range;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.InPort;

public final class AddressedItem {
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
    return "AddressedItem{" +
            "item=" + item +
            ", port=" + port +
            '}';
  }
}
