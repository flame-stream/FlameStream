package com.spbsu.datastream.example.usercounter;

import com.spbsu.datastream.core.DataItem;

import java.io.Serializable;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class UserGrouping implements DataItem.Grouping, Serializable {
  public long hash(DataItem item) {
    //noinspection ConstantConditions
    return item.as(UserContainer.class).user().hashCode();
  }

  @Override
  public boolean equals(DataItem left, DataItem right) {
    //noinspection ConstantConditions
    return left.as(UserContainer.class).user().equals(right.as(UserContainer.class).user());
  }
}
