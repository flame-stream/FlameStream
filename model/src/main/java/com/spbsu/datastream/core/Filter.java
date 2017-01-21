package com.spbsu.datastream.core;

import java.util.function.Function;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public interface Filter<T, R> extends Function<T, R> {
  boolean processOutputByElement();
}
