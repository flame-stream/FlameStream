package com.spbsu.datastream.core.meta;

import com.spbsu.datastream.core.GlobalTime;

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

