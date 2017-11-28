package com.spbsu.flamestream.runtime.node.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface VertexMaterialization extends Consumer<DataItem<?>> {
  void onMinTime(GlobalTime globalTime);

  void onCommit();
}
