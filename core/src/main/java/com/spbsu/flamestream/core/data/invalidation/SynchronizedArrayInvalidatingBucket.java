package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.stream.Stream;

public class SynchronizedArrayInvalidatingBucket extends ArrayInvalidatingBucket {
  public SynchronizedArrayInvalidatingBucket() {
    super(Collections.synchronizedList(new ArrayList<>()));
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
  public synchronized Stream<DataItem> stream() {
    return super.stream();
  }

  @Override
  public synchronized Stream<DataItem> rangeStream(int fromIndex, int toIndex) {
    return super.rangeStream(fromIndex, toIndex);
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
    return new ArrayInvalidatingBucket(Collections.synchronizedList(new ArrayList<>(innerList.subList(Math.max(
            start - window + 1, 0), start))));
  }
}
