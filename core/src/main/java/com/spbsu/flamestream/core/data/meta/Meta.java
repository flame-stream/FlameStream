package com.spbsu.flamestream.core.data.meta;

import com.spbsu.flamestream.core.data.meta.impl.MetaImpl;

public interface Meta extends Comparable<Meta> {
  static Meta meta(GlobalTime time) {
    return new MetaImpl(time);
  }

   GlobalTime globalTime();

  Trace trace();

  Meta advanced(int newLocalTime);

  Meta advanced(int newLocalTime, int childId);

  boolean isInvalidatedBy(Meta that);
}

