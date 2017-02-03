package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.example.invertedindex.WordContainer;

/**
 * Author: Artem
 * Date: 18.01.2017
 */
public class WordGrouping implements DataItem.Grouping {
  @Override
  public long hash(DataItem item) {
    //noinspection ConstantConditions
    return item.as(WordContainer.class).word().hashCode();
  }

  @Override
  public boolean equals(DataItem left, DataItem right) {
    //noinspection ConstantConditions
    return left.as(WordContainer.class).word().equals(right.as(WordContainer.class).word());
  }
}