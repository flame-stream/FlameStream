package experiments.interfaces.solar;

import java.util.stream.Stream;

/**
 * Experts League
 * Created by solar on 03.11.16.
 */
public interface Joba {
  DataType generates();
  int id();
  Stream<DataItem> materialize(Stream<DataItem> input);

  abstract class Stub implements Joba {
    private final DataType generates;
    private final int id;

    protected Stub(DataType generates) {
      this.generates = generates;
      this.id = Output.instance().registerJoba(this);
    }

    @Override
    public DataType generates() {
      return generates;
    }

    @Override
    public int id() {
      return id;
    }
  }
}
