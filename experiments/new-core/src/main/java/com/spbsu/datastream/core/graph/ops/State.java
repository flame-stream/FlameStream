package com.spbsu.datastream.core.graph.ops;

/**
 * Created by marnikitta on 2/7/17.
 */
public interface State<T> {
  void update(T value);
}
