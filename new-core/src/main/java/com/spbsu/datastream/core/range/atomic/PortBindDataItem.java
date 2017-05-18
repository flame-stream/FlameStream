package com.spbsu.datastream.core.range.atomic;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.InPort;

public final class PortBindDataItem {
  private final DataItem<?> payload;

  private final InPort inPort;

  public PortBindDataItem(DataItem<?> payload, InPort inPort) {
    this.payload = payload;
    this.inPort = inPort;
  }

  public DataItem<?> payload() {
    return this.payload;
  }

  public InPort inPort() {
    return this.inPort;
  }

  @Override
  public String toString() {
    return "PortBindDataItem{" + "payload=" + this.payload +
            ", inPort=" + this.inPort +
            '}';
  }
}
