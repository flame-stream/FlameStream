package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.Condition;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

/**
 * Created by marnikitta on 12/8/16.
 */
public class Variance implements TemplateMorphism {
  @Override
  public int arity() {
    return 1;
  }

  @Override
  public Set<Condition> conditionFor(int i) {
    return Collections.singleton(new HasValueField());
  }

  @Override
  public Function<Sink[], Sink> payload() {
    return sinks -> sinks[0];
  }

  @Override
  public DataType apply(DataType[] dataTypes) {
    if (dataTypes.length != 1) {
      throw new IllegalArgumentException();
    } else if (!dataTypes[0].conditions().containsAll(conditionFor(0))) {
      throw new IllegalArgumentException();
    }
    return new VarianceType(dataTypes[0]);
  }
}
