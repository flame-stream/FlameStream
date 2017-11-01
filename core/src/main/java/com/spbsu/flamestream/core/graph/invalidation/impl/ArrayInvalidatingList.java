package com.spbsu.flamestream.core.graph.invalidation.impl;

import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.graph.invalidation.InvalidatingList;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * User: Artem
 * Date: 01.11.2017
 */
public class ArrayInvalidatingList<E> extends InvalidatingList<E> {
  private final List<DataItem<E>> innerList = new ArrayList<>();

  @Override
  public int insert(DataItem<E> insertee) {
    int position = innerList.size() - 1;
    int endPosition = -1;
    { //find position
      while (position >= 0) {
        final DataItem<E> currentItem = innerList.get(position);
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
  public int size() {
    return innerList.size();
  }

  @Override
  public boolean isEmpty() {
    return innerList.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return innerList.contains(o);
  }

  @NotNull
  @Override
  public Iterator<DataItem<E>> iterator() {
    return innerList.iterator();
  }

  @NotNull
  @Override
  public Object[] toArray() {
    return innerList.toArray();
  }

  @NotNull
  @Override
  public <T> T[] toArray(@NotNull T[] a) {
    //noinspection SuspiciousToArrayCall
    return innerList.toArray(a);
  }

  @Override
  public boolean containsAll(@NotNull Collection<?> c) {
    return innerList.containsAll(c);
  }

  @Override
  public void clear() {

  }

  @Override
  public DataItem<E> get(int index) {
    return innerList.get(index);
  }

  @Override
  public int indexOf(Object o) {
    return innerList.indexOf(o);
  }

  @Override
  public int lastIndexOf(Object o) {
    return innerList.lastIndexOf(o);
  }

  @NotNull
  @Override
  public ListIterator<DataItem<E>> listIterator() {
    return innerList.listIterator();
  }

  @NotNull
  @Override
  public ListIterator<DataItem<E>> listIterator(int index) {
    return innerList.listIterator(index);
  }

  @NotNull
  @Override
  public List<DataItem<E>> subList(int fromIndex, int toIndex) {
    return innerList.subList(fromIndex, toIndex);
  }
}
