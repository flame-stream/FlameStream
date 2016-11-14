package experiments.interfaces.solar;

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

  static DataItem fromCharSeq(CharSequence line) {
    return new SerializedDataItem(line);
  }

  interface Meta<T extends Meta> extends Comparable<T> {
    int tick();

    T advance();
  }

  interface Grouping {
    long hash(DataItem item);

    boolean equals(DataItem left, DataItem right);
  }
}
