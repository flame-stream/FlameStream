package com.spbsu.experiments.inverted_index.common_bl.io;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqReader;

import java.io.IOException;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public abstract class Object2JSONConversionPack<T> implements ConversionPack<T, CharSeq> {
  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  protected static class To<T> implements TypeConverter<T, CharSeq> {
    private final Class<T> clazz;

    protected To(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public CharSeq convert(T userQuery) {
      try {
        return CharSeq.create(OBJECT_MAPPER.writeValueAsString(userQuery));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected static class From<T> implements TypeConverter<CharSeq, T> {
    private final Class<T> clazz;

    protected From(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public T convert(CharSeq charSeq) {
      try {
        return OBJECT_MAPPER.readValue(new CharSeqReader(charSeq), clazz);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
