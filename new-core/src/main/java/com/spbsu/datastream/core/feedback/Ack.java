package com.spbsu.datastream.core.feedback;

public class Ack {
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
