package com.spbsu.flamestream.runtime.acker.table;

import java.util.ArrayList;
import java.util.List;

public final class ArrayAckTable implements AckTable {
  private final long[] xors;
  private final int window;

  private int headPosition;
  private long headValue;

  private long maxHeartbeat;

  private final List<Long> acks = new ArrayList<>();

  public ArrayAckTable(long headValue, int capacity, int window) {
    this.window = window;
    this.xors = new long[capacity];
    this.headValue = headValue;
    this.maxHeartbeat = headValue;
    this.headPosition = 0;
  }

  @Override
  public boolean ack(long timestamp, long xor) {
    final int headOffset = Math.floorDiv(Math.toIntExact(timestamp - headValue), window);
    if (headOffset < 0) {
      throw new IllegalArgumentException("Acking back in time");
    } else if (headOffset > xors.length) {
      throw new IllegalArgumentException("Ring buffer overflow");
    } else {
      xors[(headPosition + headOffset) % xors.length] ^= xor;
      if (xors[(headPosition + headOffset) % xors.length] == 0) {
        updateHead();
        return true;
      } else {
        return false;
      }
    }
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
    return headValue;
  }

  private void updateHead() {
    int steps = 0;
    while (xors[headPosition] == 0 && headValue <= (maxHeartbeat - window)) {
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
