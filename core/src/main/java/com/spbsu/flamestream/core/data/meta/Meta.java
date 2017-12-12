package com.spbsu.flamestream.core.data.meta;

public interface Meta extends Comparable<Meta> {
  static Meta meta(GlobalTime time) {
    return new MetaImpl(time);
  }

  static Meta advanced(Meta meta, int newLocalTime) {
    return advanced(meta, newLocalTime, 0);
  }

  static Meta advanced(Meta meta, int newLocalTime, int childId) {
    return new MetaImpl(meta.globalTime(), meta.trace().advanced(newLocalTime, childId));
  }

  GlobalTime globalTime();

  Trace trace();

  boolean isInvalidatedBy(Meta that);
}

