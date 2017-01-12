package com.spbsu.datastream.core.job.control;

import com.spbsu.datastream.core.Condition;

/**
 * Created by Artem on 13.11.2016.
 */
public class ConditionTriggered extends Control {
  private Condition condition;

  public ConditionTriggered(Condition condition) {
    this.condition = condition;
  }

  public Condition condition() {
    return condition;
  }
}
