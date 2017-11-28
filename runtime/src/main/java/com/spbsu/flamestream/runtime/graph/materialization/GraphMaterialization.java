package com.spbsu.flamestream.runtime.graph.materialization;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.graph.materialization.api.AddressedItem;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public interface GraphMaterialization extends AutoCloseable {
  void accept(DataItem<?> dataItem);

  void inject(AddressedItem addressedItem);

  void onMinTimeUpdate(GlobalTime globalTime);

  void onCommit();

  @Override
  default void close() throws Exception {
  }
}
