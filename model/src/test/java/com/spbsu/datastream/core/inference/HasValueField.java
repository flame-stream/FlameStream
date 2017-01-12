package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.Condition;

/**
 * Created by marnikitta on 12/8/16.
 */
public class HasValueField implements Condition {
  @Override
  public Condition create() {
    return new HasValueField();
  }

  @Override
  public boolean update(Object item) {
    return true;
  }

  @Override
  public boolean success() {
    return false;
  }
}
