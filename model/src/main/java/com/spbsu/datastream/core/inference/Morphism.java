package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.FilterJoba;
import com.spbsu.datastream.core.job.Joba;

import java.util.function.Function;

/**
 * Created by marnikitta on 28.11.16.
 */
public interface Morphism extends Function<Joba, Joba> {
  DataType consumes();

  DataType supplies();
}
