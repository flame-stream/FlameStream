package com.spbsu.datastream.core.graph.impl;

/**
 * Created by marnikitta on 2/7/17.
 */
public interface Hash<T> {
  int hash(T o);

  boolean equals(T left, T right);
}
