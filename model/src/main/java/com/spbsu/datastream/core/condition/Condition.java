package com.spbsu.datastream.core.condition;

/**
 * Created by Artem on 01.12.2016.
 */
public interface Condition<S extends ConditionState> {
  Class<S> conditionState();
}
