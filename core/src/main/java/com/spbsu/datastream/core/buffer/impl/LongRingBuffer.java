package com.spbsu.datastream.core.buffer.impl;

import com.spbsu.datastream.core.buffer.LongBuffer;

/**
 * User: Artem
 * Date: 31.08.2017
 */
public class LongRingBuffer implements LongBuffer {
  private final long[] buffer;
  private int offset = 0;
  private int filled = 0;

  public LongRingBuffer(int capacity) {
    buffer = new long[capacity];
  }

  @Override
  public void removeFirst() {
    filled--;
  }

  @Override
  public long get(int position) {
    return buffer[(offset + (buffer.length - filled + position)) % buffer.length];
  }

  @Override
  public void put(int position, long value) {
    if (position >= filled) {
      offset = (offset + (position + 1 - filled)) % buffer.length;
      filled = position + 1;
    }
    buffer[(offset + (buffer.length - filled + position)) % buffer.length] = value;
  }

  @Override
  public int size() {
    return filled;
  }

  @Override
  public boolean isEmpty() {
    return filled == 0;
  }
}
