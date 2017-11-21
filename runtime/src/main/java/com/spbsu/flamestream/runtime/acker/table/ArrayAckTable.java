package com.spbsu.flamestream.runtime.acker.table;

public final class ArrayAckTable extends HeartbeatAckTable {
  private final long[] xorStorage;
  private int minPosition;

  public ArrayAckTable(long startTs, long stopTs, long window) {
    super(startTs, window);
    final int xorStorageSize = Math.toIntExact((stopTs - startTs) / window);
    this.xorStorage = new long[xorStorageSize];
    this.minPosition = xorStorageSize;
  }

  @Override
  public boolean ack(long timestamp, long xor) {
    final int position = Math.toIntExact(((timestamp - startTs) / window));
    final long updatedXor = xor ^ xorStorage[position];
    xorStorage[position] = updatedXor;

    if (updatedXor == 0 && xor != 0 && position == minPosition) {
      while (minPosition < xorStorage.length && xorStorage[minPosition] == 0) {
        this.minPosition++;
      }
      return true;
    } else if (updatedXor != 0 && position < minPosition) {
      this.minPosition = position;
      return true;
    } else {
      return false;
    }
  }

  @Override
  protected long tableMin() {
    return startTs + window * minPosition;
  }
}
