package com.spbsu.datastream.core.job;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.item.ObjectDataItem;
import com.spbsu.datastream.core.job.control.Control;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class FilterJoba extends Joba.Stub {
  private final Function func;
  private final Class blInput;
  private final Class blOutput;
  private final Sink sink;

  public FilterJoba(Sink sink, DataType generates, Function func, Class blInput, Class blOutput) {
    super(generates);
    this.func = func;
    this.blInput = blInput;
    this.blOutput = blOutput;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem item) {
    //noinspection unchecked
    final Object result = func.apply(item.as(blInput));
    if (result != null)
      if (result instanceof Stream) {
        //noinspection unchecked
        ((Stream)result).forEach(streamItem -> sink.accept(new ObjectDataItem(streamItem, blOutput, item.meta())));
      } else {
        sink.accept(new ObjectDataItem(result, blOutput, item.meta()));
      }
  }

  @Override
  public void accept(Control control) {
    sink.accept(control);
  }
}
