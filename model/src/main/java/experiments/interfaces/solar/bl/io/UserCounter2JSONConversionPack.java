package experiments.interfaces.solar.bl.io;

import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import experiments.interfaces.solar.bl.UserCounter;
import experiments.interfaces.solar.bl.UserQuery;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class UserCounter2JSONConversionPack extends Object2JSONConversionPack<UserCounter> {
  @Override
  public Class<? extends TypeConverter<UserCounter, CharSeq>> to() {
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSeq, UserCounter>> from() {
    return From.class;
  }

  public static class From extends Object2JSONConversionPack.From<UserCounter> {
    public From() {
      super(UserCounter.class);
    }
  }

  public static class To extends Object2JSONConversionPack.To<UserCounter> {
    public To() {
      super(UserCounter.class);
    }
  }
}
