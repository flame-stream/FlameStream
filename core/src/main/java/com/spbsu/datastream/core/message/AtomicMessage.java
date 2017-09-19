package com.spbsu.datastream.core.message;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.message.Message;

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
    return this.hash;
  }

  public InPort port() {
    return this.port;
  }

  @Override
  public T payload() {
    return this.payload;
  }

  @Override
  public long tick() {
    return this.tick;
  }

  @Override
  public String toString() {
    return "AtomicMessage{" + "tick=" + this.tick +
            ", hash=" + this.hash +
            ", port=" + this.port +
            ", payload=" + this.payload +
            '}';
  }
}

