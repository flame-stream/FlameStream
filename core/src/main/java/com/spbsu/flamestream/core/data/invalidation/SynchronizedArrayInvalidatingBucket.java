package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class SynchronizedArrayInvalidatingBucket extends ArrayInvalidatingBucket {
  public SynchronizedArrayInvalidatingBucket() {
    super(new ArrayList<>());
  }

  private SynchronizedArrayInvalidatingBucket(List<DataItem> list) {
    super(list);
  }

  @Override
  public synchronized void insert(DataItem insertee) {
    super.insert(insertee);
  }

  @Override
  public synchronized DataItem get(int index) {
    return super.get(index);
  }

  @Override
  public synchronized void forRange(int fromIndex, int toIndex, Consumer<DataItem> consumer) {
    super.forRange(fromIndex, toIndex, consumer);
  }

  @Override
  public synchronized void clearRange(int fromIndex, int toIndex) {
    super.clearRange(fromIndex, toIndex);
  }

  @Override
  public synchronized int size() {
    return super.size();
  }

  @Override
  public synchronized boolean isEmpty() {
    return super.isEmpty();
  }

  @Override
  public synchronized int lowerBound(Meta meta) {
    return super.lowerBound(meta);
  }

  @Override
  public synchronized InvalidatingBucket subBucket(Meta meta, int window) {
    final int start = lowerBound(meta);
    return new SynchronizedArrayInvalidatingBucket(new ArrayList<>(innerList.subList(Math.max(
            start - window + 1, 0), start)));
  }
}
