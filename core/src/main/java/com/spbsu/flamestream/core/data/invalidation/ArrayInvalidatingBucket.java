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
    //System.out.format("AIB.insert %s di: %s%n", System.identityHashCode(innerList), innerList.size());
    //innerList.stream().forEach(e -> {
    //  System.out.format(">>> %s%n", e);
    //});

    if (!insertee.meta().isTombstone()) {
      //System.out.println("IIIIIIIIII: " + insertee);
      final int position = lowerBound(insertee.meta());

      //System.out.format("AIB.insert is NOT tombstone %d %s%n", position, insertee);
      //final int position = lowerBound(insertee.meta());
      innerList.add(position, insertee);
    } else {
      //System.out.println("TTTTTTTTTTTT: " + insertee);
      final int position = lowerBound(insertee.meta()) - 1;
      //System.out.format("ins: %s%n", insertee);
      //System.out.format("pos-1: %d %s%n", position, innerList.get(position).meta());
      //if (position < innerList.size() - 1) {
      //  System.out.format("pos-2: %d %s%n", position + 1, innerList.get(position + 1).meta());
      //}
      //System.out.println("m1 data: " + innerList.get(position).payload(Object.class));
      //System.out.println("m2 data: " + insertee.payload(Object.class));
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
