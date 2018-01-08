package com.spbsu.flamestream.core.data.invalidation;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.stream.Stream;

public interface InvalidatingBucket {

  /**
   * Inserts data item in list according to its meta
   *
   * @return position of inserted item within list
   */
  int insert(DataItem dataItem);

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
   * Returns the index of the greatest element in this bucket less than or equal to
   * the given element
   *
   * @param meta the meta to match
   * @return the index of the greatest element less than or equal to {@code meta},
   * @throws NullPointerException if the specified meta is null
   */
  int floor(Meta meta);
}
