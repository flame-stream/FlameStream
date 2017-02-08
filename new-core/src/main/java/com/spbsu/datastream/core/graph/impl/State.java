package com.spbsu.datastream.core.graph.impl;

/**
 * Created by marnikitta on 2/7/17.
 */
public interface State<T> {
  void update(T value);
}
