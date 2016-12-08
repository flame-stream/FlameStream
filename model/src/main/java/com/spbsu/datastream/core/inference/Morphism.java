package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;

import java.util.function.Function;

/**
 * Created by marnikitta on 12/8/16.
 */
public interface Morphism extends Function<Stage, Stage> {
  boolean isConsumable(DataType type);

  boolean isProducible(DataType type);

  DataType produces(DataType consumed);

  DataType consumes(DataType produced);
}
