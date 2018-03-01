package com.spbsu.flamestream.runtime.state;

import com.expleague.commons.util.Pair;
import org.agrona.collections.IntObjConsumer;

import java.util.stream.Stream;

public interface StateStorage {
  void keys(String opId, int from, int to, IntObjConsumer<byte[]> consumer);

  void put(String opId, int hash, byte[] value);
}
