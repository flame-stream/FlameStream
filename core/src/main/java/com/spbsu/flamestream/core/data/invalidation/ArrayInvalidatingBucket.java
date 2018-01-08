package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 01.11.2017
 */
public class ArrayInvalidatingBucket implements InvalidatingBucket {
  private final List<DataItem> innerList = new ArrayList<>();

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
  public Stream<DataItem> stream() {
    return innerList.stream();
  }

  @Override
  public Stream<DataItem> rangeStream(int fromIndex, int toIndex) {
    return innerList.subList(fromIndex, toIndex).stream();
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
}
