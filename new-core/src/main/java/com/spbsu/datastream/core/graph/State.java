package com.spbsu.datastream.core.graph;

/**
 * Created by marnikitta on 2/7/17.
 */
public interface State<T> {
  void update(T value);
}
