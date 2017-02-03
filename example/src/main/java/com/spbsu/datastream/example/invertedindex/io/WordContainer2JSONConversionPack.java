package com.spbsu.datastream.example.invertedindex.io;

import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.datastream.example.invertedindex.WordContainer;
import com.spbsu.datastream.example.sql.Object2JSONConversionPack;

/**
 * Author: Artem
 * Date: 22.01.2017
 */
public class WordContainer2JSONConversionPack extends Object2JSONConversionPack<WordContainer> {
  @Override
  public Class<? extends TypeConverter<WordContainer, CharSeq>> to() {
    return WordContainer2JSONConversionPack.To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSeq, WordContainer>> from() {
    return WordContainer2JSONConversionPack.From.class;
  }

  public static class From extends Object2JSONConversionPack.From<WordContainer> {
    public From() {
      super(WordContainer.class);
    }
  }

  public static class To extends Object2JSONConversionPack.To<WordContainer> {
    public To() {
      super(WordContainer.class);
    }
  }
}