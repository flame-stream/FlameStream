package com.spbsu.datastream.example.bl.counter;

import com.spbsu.datastream.core.Condition;

/**
 * Created by Artem on 12.11.2016.
 */
public class UserMaxCountCondition implements Condition<UserCounter> {
  private int maxUserCount;

  public UserMaxCountCondition(int maxUserCount) {
    this.maxUserCount = maxUserCount;
  }

  @Override
  public boolean update(UserCounter item) {
    return item.count() <= maxUserCount;
  }

  @Override
  public boolean isFinished() {
    return false;
  }

  @Override
  public Condition create() {
    return new UserMaxCountCondition(maxUserCount);
  }
}
