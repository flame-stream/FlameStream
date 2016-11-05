package experiments.interfaces.solar;


import java.util.Set;
import java.util.stream.Stream;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface DataStream {
  DataType type();

  boolean isValid();
  double alpha();

  Set<Condition> violated();
  Stream<DataItem> stream(Stream<DataItem> stream);

  static DataStream merge(DataStream... streams) {
    return null;
  }
  static DataStream group(DataStream input, DataItem.Grouping hash) {
    return null;
  }

}
