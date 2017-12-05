package com.spbsu.flamestream.runtime.graph.materialization;

import com.spbsu.flamestream.core.DataItem;

/**
 * User: Artem
 * Date: 05.12.2017
 */
public interface Router {
  void route(DataItem<?> dataItem, Materializer.Destination destination);
}
