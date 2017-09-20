package com.spbsu.datastream.core.message;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.InPort;

public final class AtomicMessage<T extends DataItem<?>> implements Message<T> {
  private final long tick;

  private final int hash;

  private final InPort port;

  private final T payload;

  public AtomicMessage(long tick, int hash, InPort port, T payload) {
    this.tick = tick;
    this.hash = hash;
    this.port = port;
    this.payload = payload;
  }

  public int hash() {
    return hash;
  }

  public InPort port() {
    return port;
  }

  @Override
  public T payload() {
    return payload;
  }

  @Override
  public long tick() {
    return tick;
  }

  @Override
  public String toString() {
    return "AtomicMessage{" + "tick=" + tick +
            ", hash=" + hash +
            ", port=" + port +
            ", payload=" + payload +
            '}';
  }
}

