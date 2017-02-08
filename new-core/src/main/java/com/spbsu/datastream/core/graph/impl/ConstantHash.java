package com.spbsu.datastream.core.graph.impl;

import com.spbsu.datastream.core.graph.impl.Hash;

/**
 * Created by marnikitta on 2/8/17.
 */
public class ConstantHash<T> implements Hash<T> {

  @Override
  public int hash(final T o) {
    return 322223322;
  }

  @Override
  public boolean equals(final T left, final T right) {
    return true;
  }
}
