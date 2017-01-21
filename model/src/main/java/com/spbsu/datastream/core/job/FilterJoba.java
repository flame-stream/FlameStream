package com.spbsu.datastream.core.job;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.Filter;
import com.spbsu.datastream.core.Sink;
import com.spbsu.datastream.core.item.ObjectDataItem;
import com.spbsu.datastream.core.job.control.Control;

import java.util.Collection;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class FilterJoba extends Joba.Stub {
  private final Filter filter;
  private final Class blInput;
  private final Class blOutput;
  Sink sink;

  public FilterJoba(Sink sink, DataType generates, Filter filter, Class blInput, Class blOutput) {
    super(generates);
    this.filter = filter;
    this.blInput = blInput;
    this.blOutput = blOutput;
    this.sink = sink;
  }

  @Override
  public void accept(DataItem item) {
    //noinspection unchecked
    final Object result = filter.apply(item.as(blInput));
    if (result != null)
      if (filter.processOutputByElement()) {
        if (result.getClass().isArray()) {
          for (Object obj : (Object[]) result) {
            sink.accept(new ObjectDataItem(obj, blOutput.getComponentType(), item.meta()));
          }
        } else if (result instanceof Collection) {
          for (Object obj : (Collection) result) {
            sink.accept(new ObjectDataItem(obj, blOutput.getComponentType(), item.meta()));
          }
        } else {
          throw new IllegalStateException("Result object is neither array nor collection");
        }
      } else {
        sink.accept(new ObjectDataItem(result, blOutput, item.meta()));
      }
  }

  @Override
  public void accept(Control control) {
    sink.accept(control);
  }
}
