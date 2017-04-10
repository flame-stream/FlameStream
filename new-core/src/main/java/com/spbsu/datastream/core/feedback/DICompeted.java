package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.HashFunction;

public class DICompeted {
  public final static HashFunction<DICompeted> HASH_FUNCTION = new HashFunction<DICompeted>() {
    @Override
    public boolean equal(final DICompeted o1, final DICompeted o2) {
      return o1.rootHash() == o2.rootHash();
    }

    @Override
    public int hash(final DICompeted value) {
      return value.rootHash();
    }
  };

  private final long globalTs;

  private final int rootHash;

  public DICompeted(final long globalTs, final int rootHash) {
    this.globalTs = globalTs;
    this.rootHash = rootHash;
  }

  public int rootHash() {
    return rootHash;
  }

  public long globalTs() {
    return globalTs;
  }
}
