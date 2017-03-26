package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.DataItem;

public class AddressedMessage {
  private final long port;

  private final int hash;

  private final boolean isBroadcast;

  private final DataItem<?> payload;

  public AddressedMessage(final DataItem<?> payload,
                          final long port,
                          final int hash) {
    this.port = port;
    this.payload = payload;
    this.isBroadcast = false;
    this.hash = hash;
  }

  public AddressedMessage(final DataItem<?> payload,
                          final long port) {
    this.port = port;
    this.payload = payload;
    this.isBroadcast = true;
    this.hash = 0xFFFFFFFF;
  }

  public int hash() {
    return hash;
  }

  public boolean isBroadcast() {
    return isBroadcast;
  }

  public long port() {
    return port;
  }

  public DataItem<?> payload() {
    return payload;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("AddressedMessage{");
    sb.append("port=").append(port);
    sb.append(", hash=").append(hash);
    sb.append(", isBroadcast=").append(isBroadcast);
    sb.append(", payload=").append(payload);
    sb.append('}');
    return sb.toString();
  }
}
