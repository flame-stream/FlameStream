package com.spbsu.flamestream.runtime.acker.table;

public final class ArrayAckTable implements AckTable {
  private final long[] xors;
  private final int window;

  private int headPosition;
  private long headValue;

  public ArrayAckTable(long headValue, int capacity, int window) {
    this.window = window;
    this.xors = new long[capacity];
    this.headValue = headValue;
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
      return xors[(headPosition + headOffset) % xors.length] == 0;
    }
  }

  @Override
  public long tryPromote(long upTo) {
    int steps = 0;
    while (xors[headPosition] == 0 && headValue <= (upTo - window)) {
      headPosition = (headPosition + 1) % xors.length;
      headValue += window;
      steps++;

      //Optimize the case, when table is empty and upTo == Long.MAX_VALUE.
      if (steps == xors.length) {
        headValue = Math.max(upTo, headValue);
        headPosition = 0;
      }
    }
    return headValue;
  }
}
