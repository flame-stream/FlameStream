package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.PayloadHashDataItem;

public interface DataSink {
  void accept(PayloadHashDataItem dataItem);

  void accept(Control control);
}
