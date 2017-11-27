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
  private final List<DataItem<?>> innerList = new ArrayList<>();

  @Override
  public int insert(DataItem<?> insertee) {
    int position = innerList.size() - 1;
    int endPosition = -1;
    { //find position
      while (position >= 0) {
        final DataItem<?> currentItem = innerList.get(position);
        final int compareTo = currentItem.meta().compareTo(insertee.meta());

        if (compareTo > 0) {
          if (insertee.meta().isInvalidatedBy(currentItem.meta())) {
            return -1;
          }
          position--;
        } else {
          if (currentItem.meta().isInvalidatedBy(insertee.meta())) {
            endPosition = endPosition == -1 ? position : endPosition;
            position--;
          } else {
            break;
          }
        }
      }
    }
    { //invalidation/adding
      if (position == (innerList.size() - 1)) {
        innerList.add(insertee);
      } else {
        if (endPosition != -1) {
          innerList.set(position + 1, insertee);
          final int itemsForRemove = endPosition - position - 1;
          //subList.clear is faster if the number of items for removing >= 2
          if (itemsForRemove >= 2) {
            innerList.subList(position + 2, endPosition + 1).clear();
          } else if (itemsForRemove > 0) {
            innerList.remove(endPosition);
          }
        } else {
          innerList.add(position + 1, insertee);
        }
      }
    }
    return position + 1;
  }

  @Override
  public DataItem<?> get(int index) {
    return innerList.get(index);
  }

  @Override
  public Stream<DataItem<?>> stream() {
    return innerList.stream();
  }

  @Override
  public Stream<DataItem<?>> rangeStream(int fromIndex, int toIndex) {
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

  @Override
  public int floor(Meta meta) {
    int left = 0;
    int right = innerList.size();

    while (right - left > 1) {
      final int middle = left + (right - left) / 2;
      if (meta.compareTo(innerList.get(middle).meta()) < 0) {
        right = middle;
      } else {
        left = middle;
      }
    }

    return left;
  }
}
