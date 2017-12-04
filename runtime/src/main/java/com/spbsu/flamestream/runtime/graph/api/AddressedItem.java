package com.spbsu.flamestream.runtime.graph.api;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.runtime.graph.materialization.GraphMaterializer;

public class AddressedItem {
  private final DataItem<?> item;
  private final GraphMaterializer.Destination destination;

  public AddressedItem(DataItem<?> item, GraphMaterializer.Destination destination) {
    this.item = item;
    this.destination = destination;
  }

  public DataItem<?> item() {
    return item;
  }

  public GraphMaterializer.Destination destination() {
    return this.destination;
  }

  @Override
  public String toString() {
    return "AddressedItem{" +
            "item=" + item +
            ", destination='" + destination + '\'' +
            '}';
  }
}
