package com.spbsu.datastream.example.invertedindex.actions;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.example.invertedindex.WikiPageContainer;

/**
 * Author: Artem
 * Date: 31.01.2017
 */
public class WikiPageGrouping implements DataItem.Grouping {
  @Override
  public long hash(DataItem item) {
    //noinspection ConstantConditions
    return item.as(WikiPageContainer.class).pageId();
  }

  @Override
  public boolean equals(DataItem left, DataItem right) {
    //noinspection ConstantConditions
    return left.as(WikiPageContainer.class).pageId() == right.as(WikiPageContainer.class).pageId();
  }
}
