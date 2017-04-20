package com.spbsu.datastream.core.tick;

import com.spbsu.datastream.core.DataItem;

public final class AddressedMessage {
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

  public int hash() {
    return this.hash;
  }

  public boolean isBroadcast() {
    return this.isBroadcast;
  }

  public long port() {
    return this.port;
  }

  public DataItem<?> payload() {
    return this.payload;
  }

  @Override
  public String toString() {
    return "AddressedMessage{" + "port=" + this.port +
            ", hash=" + this.hash +
            ", isBroadcast=" + this.isBroadcast +
            ", payload=" + this.payload +
            '}';
  }
}
