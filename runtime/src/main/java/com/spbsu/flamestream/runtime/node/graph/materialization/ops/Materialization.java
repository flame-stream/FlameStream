package com.spbsu.flamestream.runtime.node.graph.materialization.ops;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface Materialization extends Function<DataItem<?>, Stream<DataItem<?>>> {
  void onMinGTimeUpdate(GlobalTime globalTime);

  void onCommit();

  abstract class Stub implements Materialization {
    @Override
    public void onMinGTimeUpdate(GlobalTime globalTime) {
      //default
    }

    @Override
    public void onCommit() {
      //default
    }
  }

}
