package com.spbsu.flamestream.runtime.node.graph.materialization.api;

import com.spbsu.flamestream.core.DataItem;

public class AddressedItem {
  private final DataItem<?> item;
  private final String vertexId;

  public AddressedItem(DataItem<?> item, String vertexId) {
    this.item = item;
    this.vertexId = vertexId;
  }

  public DataItem<?> item() {
    return item;
  }

  public String vertexId() {
    return this.vertexId;
  }

  @Override
  public String toString() {
    return "AddressedItem{" +
            "item=" + item +
            ", vertexId='" + vertexId + '\'' +
            '}';
  }
}
