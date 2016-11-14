package experiments.interfaces.solar.items;

import com.spbsu.commons.seq.CharSeq;
import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.DataTypeCollection;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class SerializedDataItem implements DataItem {
  private final Meta meta;
  private final CharSeq line;

  public SerializedDataItem(CharSequence line) {
    this.meta = new MetaImpl(System.nanoTime());
    this.line = CharSeq.create(line);
  }

  @Override
  public Meta meta() {
    return meta;
  }

  @Override
  public CharSequence serializedData() {
    return line;
  }

  @Override
  public <T> T as(Class<T> type) {
    return DataTypeCollection.SERIALIZATION.read(line, type);
  }
}
