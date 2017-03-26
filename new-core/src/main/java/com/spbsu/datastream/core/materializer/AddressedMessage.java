package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.InPort;

public class AddressedMessage {
  private final long port;

  private final int hash;

  private final boolean isBroadcast;

  private final DataItem<?> payload;

  public AddressedMessage(final DataItem<?> payload, final long port, final int hash, final boolean isBroadcast) {
    this.port = port;
    this.payload = payload;
    this.isBroadcast = isBroadcast;
    this.hash = hash;
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
    return "AddressedMessage{" + "port=" + port +
            ", hash=" + hash +
            ", isBroadcast=" + isBroadcast +
            ", payload=" + payload +
            '}';
  }
}
