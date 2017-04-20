package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.HashFunction;

public final class Ack {
  public static final HashFunction<Ack> HASH_FUNCTION = new HashFunction<Ack>() {
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
    return this.rootHash;
  }

  public long globalTs() {
    return this.globalTs;
  }

  public long ack() {
    return this.ack;
  }
}
