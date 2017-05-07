package com.spbsu.datastream.core.configuration;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;

public final class HashRangeDeserializer extends KeyDeserializer {

  @Override
  public Object deserializeKey(final String s,
                               final DeserializationContext deserializationContext) {
    final String[] arr = s.split("_");
    return new HashRange(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]));
  }
}
