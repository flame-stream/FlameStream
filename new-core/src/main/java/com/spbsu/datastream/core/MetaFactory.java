package com.spbsu.datastream.core;

import java.util.function.Supplier;

/**
 * Created by marnikitta on 2/5/17.
 */
public interface MetaFactory<T extends DataItem.Meta> extends Supplier<T> {
  @Override
  T get();

  T advanced(T advanced);
}
