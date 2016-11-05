package experiments.interfaces.solar.bl.io;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.commons.func.types.ConversionPack;
import com.spbsu.commons.func.types.TypeConverter;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.commons.seq.CharSeqReader;
import com.spbsu.commons.util.JSONTools;
import experiments.interfaces.solar.bl.UserQuery;

import java.io.IOException;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class Object2JSONConversionPack<T> implements ConversionPack<T, CharSeq> {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private Class<T> clazz;

  protected Object2JSONConversionPack(Class<T> clazz) {
    this.clazz = clazz;
  }

  private class To implements TypeConverter<T, CharSeq> {
    @Override
    public CharSeq convert(T userQuery) {
      try {
        return CharSeq.create(OBJECT_MAPPER.writeValueAsString(userQuery));
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }
  private class From implements TypeConverter<CharSeq, T> {
    @Override
    public T convert(CharSeq charSeq) {
      try {
        return OBJECT_MAPPER.readValue(new CharSeqReader(charSeq), clazz);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Class<? extends TypeConverter<T, CharSeq>> to() {
    //noinspection unchecked
    return To.class;
  }

  @Override
  public Class<? extends TypeConverter<CharSeq, T>> from() {
    //noinspection unchecked
    return From.class;
  }
}
