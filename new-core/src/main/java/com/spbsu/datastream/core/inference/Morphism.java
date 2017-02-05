package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;

import java.util.function.Function;

/**
 * Created by marnikitta on 12/8/16.
 */
public interface Morphism extends Function<DataType[], DataType> {
  int arity();

  boolean isConsumed(DataType[] domain);

  boolean isConsumed(int place, DataType domain);

  boolean isProduced(DataType codomain);

  Function<Object, Object[]> wrapSink();
}
