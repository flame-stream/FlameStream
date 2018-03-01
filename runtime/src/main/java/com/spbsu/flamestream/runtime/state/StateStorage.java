package com.spbsu.flamestream.runtime.state;

import org.agrona.collections.IntObjConsumer;

public interface StateStorage {
  void get(String opId, int from, int to, IntObjConsumer<byte[]> consumer);

  void put(String opId, int hash, byte[] value);
}
