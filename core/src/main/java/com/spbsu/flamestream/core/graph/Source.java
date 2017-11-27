package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;

/**
 * User: Artem
 * Date: 21.11.2017
 */
public class Source<T> implements Graph.Vertex<T> {
  private final HashFunction<? super T> hash;

  public Source(HashFunction<? super T> hash) {
    this.hash = hash;
  }

  @Override
  public HashFunction<? super T> inputHash() {
    return hash;
  }
}
