package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.HashFunction;

public class Ack {
  public final static HashFunction<Ack> HASH_FUNCTION = new HashFunction<Ack>() {
    @Override
    public boolean equal(final Ack o1, final Ack o2) {
      return o1.rootHash() == o2.rootHash();
    }

    @Override
    public int hash(final Ack value) {
      return value.rootHash();
    }
  };

  private final long globalTs;

  private final long ack;

  private final int rootHash;

  public Ack(final long globalTs, final int rootHash, final long ack) {
    this.globalTs = globalTs;
    this.ack = ack;
    this.rootHash = rootHash;
  }

  public int rootHash() {
    return rootHash;
  }

  public long globalTs() {
    return globalTs;
  }

  public long ack() {
    return ack;
  }
}
