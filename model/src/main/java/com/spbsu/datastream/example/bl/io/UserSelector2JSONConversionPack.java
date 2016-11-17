package com.spbsu.datastream.example.bl.io;

import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.datastream.example.bl.sql.UserSelector;

/**
 * Created by Artem on 15.11.2016.
 */
public class UserSelector2JSONConversionPack extends Object2JSONConversionPack<UserSelector> {
  @Override
  public Class<? extends TypeConverter<UserSelector, CharSeq>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSeq, UserSelector>> from() {
    return From.class;
  }

  public static class From extends Object2JSONConversionPack.From<UserSelector> {
    public From() {
      super(UserSelector.class);
    }
  }

  public static class To extends Object2JSONConversionPack.To<UserSelector> {
    public To() {
      super(UserSelector.class);
    }
  }
}
