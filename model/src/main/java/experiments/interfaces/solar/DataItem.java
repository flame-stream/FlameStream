package experiments.interfaces.solar;

import com.spbsu.commons.func.types.ConversionRepository;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.seq.CharSeq;
import experiments.interfaces.solar.items.MetaImpl;
import experiments.interfaces.solar.items.SerializedDataItem;
import org.jetbrains.annotations.Nullable;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface DataItem {

  Meta meta();
  CharSequence serializedData();
  @Nullable
  <T> T as(Class<T> type);

  static DataItem fromCharSeq(CharSeq line) {
    return new SerializedDataItem(line);
  }

  interface Meta {
    SystemTime time();
    int tick();

    static Meta advance(Meta meta, int id) {
      return new MetaImpl(meta, id);
    }
  }

  interface Grouping {
    long hash(DataItem item);
    boolean equals(DataItem left, DataItem right);
  }
}
