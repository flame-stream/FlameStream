package com.spbsu.flamestream.runtime.serialization;

public interface FlameSerializer {
  byte[] serialize(Object o);

  <T> T deserialize(byte[] data, Class<T> clazz);
}
