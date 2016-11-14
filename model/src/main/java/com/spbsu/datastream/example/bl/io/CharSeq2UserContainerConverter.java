package com.spbsu.datastream.example.bl.io;

import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.datastream.example.bl.UserContainer;

import java.io.IOException;

/**
 * Experts League
 * Created by solar on 07.11.16.
 */
public class CharSeq2UserContainerConverter implements TypeConverter<CharSequence, UserContainer> {
  @Override
  public UserContainer convert(CharSequence charSequence) {
    try {
      return Object2JSONConversionPack.OBJECT_MAPPER.readValue(new CharSeqReader(CharSeq.create(charSequence)), UserContainer.class);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
