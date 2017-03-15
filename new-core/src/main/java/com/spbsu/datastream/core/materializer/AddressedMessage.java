package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.Traveler;
import com.spbsu.datastream.core.graph.InPort;

/**
 * Created by marnikitta on 2/8/17.
 */
public class AddressedMessage<T extends Traveler> {
  private final InPort port;

  private final T payload;

  public AddressedMessage(final T payload, final InPort port) {
    this.port = port;
    this.payload = payload;
  }

  public InPort port() {
    return port;
  }

  public T payload() {
    return payload;
  }

  @Override
  public String toString() {
    return "AddressedMessage{" + "port=" + port +
            ", payload=" + payload +
            '}';
  }
}
