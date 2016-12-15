package com.spbsu.datastream.core.job.control;

import com.spbsu.datastream.core.Condition;

/**
 * Created by Artem on 13.11.2016.
 */
public class ConditionTriggered extends Control {
  public ConditionTriggered(Condition condition) {
    System.out.println("Triggered condition: " + condition.toString());
  }
}
