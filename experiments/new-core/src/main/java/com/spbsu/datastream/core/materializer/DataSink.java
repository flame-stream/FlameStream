package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.DataItem;

public interface DataSink {
  void accept(DataItem dataItem);

  void accept(Control control);
}
