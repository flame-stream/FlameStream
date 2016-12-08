package com.spbsu.datastream.example.bl.counter;

import com.spbsu.datastream.core.condition.ConditionEmptyState;
import com.spbsu.datastream.core.condition.FailCondition;

/**
 * Created by Artem on 12.11.2016.
 */
public class UserMaxCountCondition implements FailCondition<UserCounter, ConditionEmptyState> {
  private int maxUserCount;

  public UserMaxCountCondition(int maxUserCount) {
    this.maxUserCount = maxUserCount;
  }

  @Override
  public boolean taskFail(UserCounter item, ConditionEmptyState state) {
    return item.count() > maxUserCount;
  }

  @Override
  public Class<ConditionEmptyState> conditionState() {
    return ConditionEmptyState.class;
  }
}
