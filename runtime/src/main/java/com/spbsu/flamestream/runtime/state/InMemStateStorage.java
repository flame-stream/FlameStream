package com.spbsu.flamestream.runtime.state;

import org.agrona.collections.IntObjConsumer;

import java.util.SortedMap;
import java.util.TreeMap;

public class InMemStateStorage implements StateStorage {
  private final SortedMap<String, byte[]> storage = new TreeMap<>();

  @Override
  public void keys(String opId, int from, int to, IntObjConsumer<byte[]> consumer) {
    storage.subMap(String.valueOf(from) + "_" + opId, String.valueOf(to) + opId).forEach((s, bytes) -> {
      consumer.accept(Integer.valueOf(s.split("_")[0]), bytes);
    });
  }

  @Override
  public void put(String opId, int hash, byte[] value) {
    storage.put(String.valueOf(hash) + "_" + opId, value);
  }
}
