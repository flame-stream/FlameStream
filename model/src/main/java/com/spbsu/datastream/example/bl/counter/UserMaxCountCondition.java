package com.spbsu.datastream.example.bl.counter;

import com.spbsu.datastream.core.condition.FailCondition;

/**
 * Created by Artem on 12.11.2016.
 */
public class UserMaxCountCondition implements FailCondition<UserCounter> {
  private int maxUserCount;

  public UserMaxCountCondition(int maxUserCount) {
    this.maxUserCount = maxUserCount;
  }

  @Override
  public boolean stateOk(UserCounter item) {
    return item.count() <= maxUserCount;
  }
}
