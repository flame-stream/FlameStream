package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;

import java.util.function.Function;

/**
 * Created by marnikitta on 12/8/16.
 */
public interface Morphism extends Function<DataType[], DataType> {
  Function<Sink[], Sink> payload();
}
