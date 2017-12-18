package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.stream.Stream;

public interface InvalidatingBucket {

  /**
   * Inserts data item in list according to its meta
   */
  void insert(DataItem dataItem);

  /**
   * @param index within bucket
   * @return DataItem that located on the index position
   * @throws IndexOutOfBoundsException if the index is out of range
   */
  DataItem get(int index);

  /**
   * @return stream consists of elements in this bucket
   */
  Stream<DataItem> stream();

  /**
   * @param fromIndex low endpoint (inclusive) of the range
   * @param toIndex   high endpoint (exclusive) of the range
   * @return a stream of the specified range within this bucket
   * @throws IndexOutOfBoundsException if at least one index is out of range
   */
  Stream<DataItem> rangeStream(int fromIndex, int toIndex);

  /**
   * Removes the elements of bucket between fromIndex and toIndex
   *
   * @param fromIndex low endpoint (inclusive) of the range
   * @param toIndex   high endpoint (exclusive) of the range
   */
  void clearRange(int fromIndex, int toIndex);

  /**
   * @return the size of this bucket
   */
  int size();

  /**
   * @return true if bucket is empty, otherwise return false
   */
  boolean isEmpty();

  /**
   * Returns an index of the first element in the bucket that is not less than (i.e. greater or equal to) value,
   * or last if no such element is found.
   * <p>
   * min i : a[i] >= x
   */
  int lowerBound(Meta meta);
}
