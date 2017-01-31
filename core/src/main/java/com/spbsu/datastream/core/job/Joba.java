package com.spbsu.datastream.core.job;

import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;

/**
 * Experts League
 * Created by solar on 03.11.16.
 */
public interface Joba extends Sink {
  DataType generates();
  int id();

  abstract class Stub implements Joba {
    private final DataType generates;
    private final int id;

    protected Stub(DataType generates) {
      this.generates = generates;
      this.id = DataStreamsContext.output.registerJoba(this);
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
