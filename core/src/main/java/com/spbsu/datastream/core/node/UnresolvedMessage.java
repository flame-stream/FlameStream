package com.spbsu.datastream.core.node;

import com.spbsu.datastream.core.message.Message;

public final class UnresolvedMessage<T extends Message<?>> {
  private final int destination;
  private final T payload;
  private final boolean broadcast;

  public UnresolvedMessage(int destination, T payload) {
    this.destination = destination;
    this.payload = payload;
    this.broadcast = false;
  }

  public UnresolvedMessage(T payload) {
    this.destination = Integer.MAX_VALUE;
    this.payload = payload;
    this.broadcast = true;
  }

  public boolean isBroadcast() {
    return broadcast;
  }

  public int destination() {
    return destination;
  }

  public T payload() {
    return payload;
  }

  @Override
  public String toString() {
    return "UnresolvedMessage{" + "destination=" + destination +
            ", payload=" + payload +
            '}';
  }
}
