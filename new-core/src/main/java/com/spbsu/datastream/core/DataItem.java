package com.spbsu.datastream.core;

public interface DataItem<T> {
  Meta meta();

  T payload();

  /**
   * HashFunction is a property of dataItem
   *
   * @return hashFunction of the item.
   */
  int hash();
}
