package com.spbsu.flamestream.runtime.node.graph.materialization.vertices;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.GlobalTime;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 27.11.2017
 */
public interface VertexMaterialization extends Function<DataItem<?>, Stream<DataItem<?>>> {
  Graph.Vertex vertex();

  void onMinGTimeUpdate(GlobalTime globalTime);

  void onCommit();

  abstract class Stub implements VertexMaterialization {
    protected final Graph.Vertex vertex;

    protected Stub(Graph.Vertex vertex) {
      this.vertex = vertex;
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
    public Graph.Vertex vertex() {
      return vertex;
    }
  }
}
