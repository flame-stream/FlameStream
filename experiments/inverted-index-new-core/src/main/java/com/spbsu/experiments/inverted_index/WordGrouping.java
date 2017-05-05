package com.spbsu.experiments.inverted_index;

import com.spbsu.datastream.core.HashFunction;
import com.spbsu.experiments.inverted_index.common_bl.models.WordContainer;

import java.io.Serializable;

public class WordGrouping implements HashFunction<WordContainer>, Serializable {

  @Override
  public boolean equal(final WordContainer o1, final WordContainer o2) {
    return o1.word().equals(o2.word());
  }

  @Override
  public int hash(final WordContainer value) {
    return value.hashCode();
  }
}
