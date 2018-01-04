package com.spbsu.flamestream.example.bl.index.ops;

import com.google.common.annotations.VisibleForTesting;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

import java.util.Arrays;

/**
 * User: Artem
 * Date: 19.03.2017
 * Time: 13:05
 */
public class InvertedIndexState {
  public static final int PREV_VALUE_NOT_FOUND = -1;
  private static final int DEFAULT_MAX_WINDOW_SIZE = 100;

  private final int maxWindowSize;
  private TLongArray[] storage;

  public InvertedIndexState() {
    this(DEFAULT_MAX_WINDOW_SIZE);
  }

  private InvertedIndexState(TLongArray[] arrayLists, int maxWindowSize) {
    storage = arrayLists;
    this.maxWindowSize = maxWindowSize;
  }

  private InvertedIndexState(int maxWindowSize) {
    if (maxWindowSize <= 1) {
      throw new IllegalArgumentException("Max window size should be > 1");
    }
    this.maxWindowSize = maxWindowSize;
    storage = new TLongArray[1];
    storage[0] = new TLongArray();
  }

  public long updateOrInsert(long[] pagePositions) {
    final long first = pagePositions[0];
    final int pageId = IndexItemInLong.pageId(first);

    final long valueForSearch = IndexItemInLong.createPagePosition(pageId, 0, 0);
    final int newPosition = IndexItemInLong.position(first);
    final int newRange = pagePositions.length;

    final long prevValue = tryToFindAndUpdate(valueForSearch, newPosition, newRange);
    if (prevValue == InvertedIndexState.PREV_VALUE_NOT_FOUND) {
      final long newValue = IndexItemInLong.setRange(first, newRange);
      insert(newValue);
    }
    return prevValue;
  }

  public InvertedIndexState copy() {
    final TLongArray[] copy = Arrays.copyOf(storage, storage.length);
    return new InvertedIndexState(copy, maxWindowSize);
  }

  @VisibleForTesting
  long tryToFindAndUpdate(long value, int newPosition, int newRange) {
    final int windowIndex = findWindow(value);
    int searchIndex = storage[windowIndex].binarySearch(value);
    if (searchIndex < 0) {
      searchIndex = -searchIndex - 1;
    }

    final long searchValue;
    if (searchIndex < storage[windowIndex].size()
            && IndexItemInLong.pageId(searchValue = storage[windowIndex].get(searchIndex)) == IndexItemInLong.pageId(value)) {
      final long newValue = IndexItemInLong.createPagePosition(
              IndexItemInLong.pageId(value),
              newPosition,
              IndexItemInLong.version(value),
              newRange
      );
      final TLongArray temp = new TLongArray();
      temp.setInner(Arrays.copyOf(storage[windowIndex].inner(), storage[windowIndex].inner().length));
      temp.setPos(storage[windowIndex].pos());
      storage[windowIndex] = temp;
      storage[windowIndex].set(searchIndex, newValue);
      return searchValue;
    } else {
      return PREV_VALUE_NOT_FOUND;
    }
  }

  @VisibleForTesting
  void insert(long value) {
    final int windowIndex = findWindow(value);
    final int insertIndex = -storage[windowIndex].binarySearch(value) - 1;
    if (insertIndex < 0) {
      throw new IllegalArgumentException("Storage contains such value");
    }

    if (storage[windowIndex].size() + 1 > maxWindowSize) {
      final TLongArray firstWindow = new TLongArray();
      final TLongArray secondWindow = new TLongArray();
      for (int i = 0; i < storage[windowIndex].size() / 2; i++) {
        firstWindow.add(storage[windowIndex].get(i));
      }
      for (int i = storage[windowIndex].size() / 2; i < storage[windowIndex].size(); i++) {
        secondWindow.add(storage[windowIndex].get(i));
      }

      final TLongArray[] newStorage = new TLongArray[storage.length + 1];
      System.arraycopy(storage, 0, newStorage, 0, windowIndex);
      System.arraycopy(
              storage,
              windowIndex + 2 - 1,
              newStorage,
              windowIndex + 2,
              newStorage.length - (windowIndex + 2)
      );
      newStorage[windowIndex] = firstWindow;
      newStorage[windowIndex + 1] = secondWindow;
      storage = newStorage;

      insert(value);
    } else {
      final TLongArray temp = new TLongArray();
      temp.setInner(Arrays.copyOf(storage[windowIndex].inner(), storage[windowIndex].inner().length));
      temp.setPos(storage[windowIndex].pos());
      storage[windowIndex] = temp;
      storage[windowIndex].insert(insertIndex, value); // TODO: 18.12.2017  #112
    }
  }

  @VisibleForTesting
  TLongList toList() {
    final TLongList result = new TLongArray();
    for (TLongArray list : storage) {
      result.addAll(list);
    }
    return result;
  }

  private int findWindow(long value) {
    int windowIndex = 0;
    while (windowIndex < storage.length && !storage[windowIndex].isEmpty() && value > storage[windowIndex].get(0)) {
      if (storage[windowIndex].get(storage[windowIndex].size() - 1) > value) {
        break;
      } else {
        windowIndex++;
      }
    }
    return windowIndex >= storage.length ? windowIndex - 1 : windowIndex;
  }

  private static class TLongArray extends TLongArrayList {
    @SuppressWarnings("WeakerAccess")
    public TLongArray() {
      super();
    }

    long[] inner() {
      return _data;
    }

    int pos() {
      return _pos;
    }

    void setInner(long[] data) {
      _data = data;
    }

    void setPos(int pos) {
      _pos = pos;
    }
  }
}
