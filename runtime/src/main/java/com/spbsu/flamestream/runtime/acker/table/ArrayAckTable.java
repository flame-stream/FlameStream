package com.spbsu.flamestream.runtime.acker.table;

public final class ArrayAckTable implements AckTable {
  private final long[] xors;
  private final long window;

  private int headPosition;
  private long headValue;

  private long maxHeartbeat;

  public ArrayAckTable(long headValue, int capacity, long window) {
    this.window = window;
    this.xors = new long[capacity];
    this.headValue = headValue;
    this.maxHeartbeat = headValue;
    this.headPosition = 0;
  }

  @Override
  public boolean ack(long timestamp, long xor) {
    if (timestamp < headValue) {
      throw new IllegalArgumentException("Acking back in time");
    }

    final int headOffset = Math.toIntExact((timestamp - headValue) / window);
    if (headOffset > xors.length) {
      throw new IllegalArgumentException("Ring buffer overflow");
    } else {
      xors[(headPosition + headOffset) % xors.length] ^= xor;
      updateHead();
    }

    return xors[(headPosition + headOffset) % xors.length] == 0;
  }

  @Override
  public void heartbeat(long timestamp) {
    if (timestamp < maxHeartbeat) {
      throw new IllegalArgumentException("Not monotonic reports");
    }
    maxHeartbeat = timestamp;
    updateHead();
  }

  @Override
  public long min() {
    return Math.min(headValue, maxHeartbeat);
  }

  private void updateHead() {
    int steps = 0;
    while (xors[headPosition] == 0 && headValue < maxHeartbeat) {
      headPosition = (headPosition + 1) % xors.length;
      headValue += window;
      steps++;

      if (steps == xors.length) {
        headValue = maxHeartbeat;
        headPosition = 0;
      }
    }
  }
}
