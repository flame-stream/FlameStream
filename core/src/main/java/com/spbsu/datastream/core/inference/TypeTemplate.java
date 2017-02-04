package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.Condition;
import com.spbsu.datastream.core.DataType;

import java.util.Set;
import java.util.function.Function;

/**
 * Created by marnikitta on 12/8/16.
 */
public interface TypeTemplate extends Function<DataType[], DataType> {
  int arity();

  Set<Condition> conditionFor(int i);
}
