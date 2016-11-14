package com.spbsu.datastream.example.bl.io;

import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.datastream.example.bl.UserQuery;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class UserQuery2JSONConversionPack extends Object2JSONConversionPack<UserQuery> {
  @Override
  public Class<? extends TypeConverter<UserQuery, CharSeq>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSeq, UserQuery>> from() {
    return From.class;
  }

  public static class From extends Object2JSONConversionPack.From<UserQuery> {
    public From() {
      super(UserQuery.class);
    }
  }

  public static class To extends Object2JSONConversionPack.To<UserQuery> {
    public To() {
      super(UserQuery.class);
    }
  }
}
