package experiments.interfaces.solar.jobas;

import experiments.interfaces.solar.DataItem;
import experiments.interfaces.solar.DataType;
import experiments.interfaces.solar.Joba;
import experiments.interfaces.solar.items.ObjectDataItem;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class IdentityJoba extends Joba.Stub {
  public IdentityJoba(DataType generates) {
    super(generates);
  }

  @Override
  public Stream<DataItem> materialize(Stream<DataItem> input) {
    return input;
  }
}
