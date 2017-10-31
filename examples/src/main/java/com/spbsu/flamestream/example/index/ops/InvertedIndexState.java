package com.spbsu.flamestream.example.index.ops;

import com.google.common.annotations.VisibleForTesting;
import com.spbsu.flamestream.example.index.utils.IndexItemInLong;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;

/**
 * User: Artem
 * Date: 19.03.2017
 * Time: 13:05
 */
public class InvertedIndexState {
  public static final int PREV_VALUE_NOT_FOUND = -1;
  private static final int DEFAULT_MAX_WINDOW_SIZE = 100;

  private final int maxWindowSize;
  private TLongArrayList[] storage;

  public InvertedIndexState() {
    this(DEFAULT_MAX_WINDOW_SIZE);
  }

  private InvertedIndexState(int maxWindowSize) {
    if (maxWindowSize <= 1) {
      throw new IllegalArgumentException("Max window size should be > 1");
    }
    this.maxWindowSize = maxWindowSize;
    storage = new TLongArrayList[1];
    storage[0] = new TLongArrayList();
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

  @VisibleForTesting
  long tryToFindAndUpdate(long value, int newPosition, int newRange) {
    final int windowIndex = findWindow(value);
    final TLongArrayList window = storage[windowIndex];
    int searchIndex = window.binarySearch(value);
    if (searchIndex < 0) {
      searchIndex = -searchIndex - 1;
    }

    final long searchValue;
    if (searchIndex < window.size()
            && IndexItemInLong.pageId(searchValue = window.get(searchIndex)) == IndexItemInLong.pageId(value)) {
      final long newValue = IndexItemInLong.createPagePosition(
              IndexItemInLong.pageId(value),
              newPosition,
              IndexItemInLong.version(value),
              newRange
      );
      window.set(searchIndex, newValue);
      return searchValue;
    } else {
      return PREV_VALUE_NOT_FOUND;
    }
  }

  @VisibleForTesting
  void insert(long value) {
    int windowIndex = findWindow(value);
    final TLongArrayList window = storage[windowIndex];
    final int insertIndex = -window.binarySearch(value) - 1;
    if (insertIndex < 0) {
      throw new IllegalArgumentException("Storage contains such value");
    }

    if (window.size() + 1 > maxWindowSize) {
      final TLongArrayList firstWindow = new TLongArrayList();
      final TLongArrayList secondWindow = new TLongArrayList();
      for (int i = 0; i < window.size() / 2; i++) {
        firstWindow.add(window.get(i));
      }
      for (int i = window.size() / 2; i < window.size(); i++) {
        secondWindow.add(window.get(i));
      }

      final TLongArrayList[] newStorage = new TLongArrayList[storage.length + 1];
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
      window.insert(insertIndex, value);
    }
  }

  @VisibleForTesting
  TLongList toList() {
    final TLongList result = new TLongArrayList();
    for (TLongArrayList list : storage) {
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
}
