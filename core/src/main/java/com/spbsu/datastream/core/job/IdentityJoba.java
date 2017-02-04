package com.spbsu.datastream.core.job;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.job.control.Control;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class IdentityJoba extends Joba.AbstractJoba {
  private final Sink sink;

  public IdentityJoba(Sink sink, DataType generates) {
    super(generates);
    this.sink = sink;
  }

  @Override
  public void accept(DataItem item) {
    sink.accept(item);
  }

  @Override
  public void accept(Control control) {
    sink.accept(control);
  }
}
