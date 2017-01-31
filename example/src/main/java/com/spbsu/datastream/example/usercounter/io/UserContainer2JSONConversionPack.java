package com.spbsu.datastream.example.usercounter.io;

import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.datastream.example.usercounter.UserContainer;
import com.spbsu.datastream.example.sql.Object2JSONConversionPack;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class UserContainer2JSONConversionPack extends Object2JSONConversionPack<UserContainer> {
  @Override
  public Class<? extends TypeConverter<UserContainer, CharSeq>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSeq, UserContainer>> from() {
    return From.class;
  }

  public static class From extends Object2JSONConversionPack.From<UserContainer> {
    public From() {
      super(UserContainer.class);
    }
  }

  public static class To extends Object2JSONConversionPack.To<UserContainer> {
    public To() {
      super(UserContainer.class);
    }
  }
}
