package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 01.11.2017
 */
public class ArrayInvalidatingBucket implements InvalidatingBucket {
  protected final List<DataItem> innerList;

  public ArrayInvalidatingBucket() {
    this(new ArrayList<>());
  }

  protected ArrayInvalidatingBucket(List<DataItem> innerList) {
    this.innerList = innerList;
  }

  @Override
  public void insert(DataItem insertee) {
    if (!insertee.meta().isTombstone()) {
      final int position = lowerBound(insertee.meta());
      innerList.add(position, insertee);
    } else {
      final int position = lowerBound(insertee.meta()) - 1;
      if (!innerList.get(position).meta().isInvalidedBy(insertee.meta())) {
        throw new IllegalStateException("There is no invalidee");
      }
      innerList.remove(position);
    }
  }

  @Override
  public DataItem get(int index) {
    return innerList.get(index);
  }

  @Override
  public void forRange(int fromIndex, int toIndex, Consumer<DataItem> consumer) {
    for (int i = fromIndex; i < toIndex; i++) {
      consumer.accept(innerList.get(i));
    }
  }

  @Override
  public void clearRange(int fromIndex, int toIndex) {
    innerList.subList(fromIndex, toIndex).clear();
  }

  @Override
  public int size() {
    return innerList.size();
  }

  @Override
  public boolean isEmpty() {
    return innerList.isEmpty();
  }

  /**
   * Lower bound as Burunduk1 says
   * <a href="http://acm.math.spbu.ru/~sk1/mm/cs-center/src/2015-09-24/bs.cpp.html"/>"/>
   */
  @Override
  public int lowerBound(Meta meta) {
    int left = 0;
    int right = innerList.size();
    while (left != right) {
      final int middle = left + (right - left) / 2;
      if (innerList.get(middle).meta().compareTo(meta) >= 0) {
        right = middle;
      } else {
        left = middle + 1;
      }
    }
    return left;
  }

  @Override
  public InvalidatingBucket subBucket(Meta meta, int window) {
    final int start = lowerBound(meta);
    return new ArrayInvalidatingBucket(new ArrayList<>(innerList.subList(Math.max(start - window + 1, 0), start)));
  }
}
