package com.spbsu.datastream.core.meta;

import com.spbsu.datastream.core.GlobalTime;

import java.util.Comparator;


public interface Meta extends Comparable<Meta> {
  GlobalTime globalTime();

  Trace trace();

  boolean isBrother(Meta that);

  Meta advanced(int newLocalTime);

  Meta advanced(int newLocalTime, int childId);

  static Meta meta(GlobalTime time) {
    return new MetaImpl(time);
  }

  Comparator<Meta> NATURAL_ORDER = Comparator
          .comparing(Meta::globalTime)
          .thenComparing(Meta::trace);
}

