package experiments.interfaces.artem.mockstream;

import experiments.interfaces.artem.mockstream.impl.MergeStream;

import java.util.Set;
import java.util.stream.Stream;

/**
 * Experts League
 * Created by solar on 17.10.16.
 */
public interface DataStream extends Stream<DataItem> {
  Type types();

  boolean isValid();

  double alpha();

  Set<Condition> violated();

  static DataStream merge(DataStream... streams) {
    return new MergeStream(streams);
  }

  static DataStream group(DataStream input, DataItem.Grouping hash) {
    return null;
  }

  interface Type {
    String name();

    Set<Class<? extends Condition>> conditions();
  }
}
