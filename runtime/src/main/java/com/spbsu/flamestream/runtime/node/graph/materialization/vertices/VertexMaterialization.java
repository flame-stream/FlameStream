package com.spbsu.flamestream.runtime.node.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface VertexMaterialization extends Function<DataItem<?>, Stream<DataItem<?>>> {
  String vertexId();

  void onMinGTimeUpdate(GlobalTime globalTime);

  void onCommit();

  abstract class Stub implements VertexMaterialization {
    final String vertexId;

    protected Stub(String vertexId) {
      this.vertexId = vertexId;
    }

    @Override
    public void onMinGTimeUpdate(GlobalTime globalTime) {
      //default
    }

    @Override
    public void onCommit() {
      //default
    }

    @Override
    public String vertexId() {
      return vertexId;
    }
  }
}
