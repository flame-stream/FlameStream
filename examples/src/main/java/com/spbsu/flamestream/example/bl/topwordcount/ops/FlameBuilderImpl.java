package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.MapOperation;
import com.spbsu.flamestream.core.graph.FlameMap;

import java.util.List;

public class FlameBuilderImpl implements FlameBuilder {
  private final Graph.Builder builder = new Graph.Builder();

  @Override
  public <Input> Graph.Vertex vertex(MapOperation<Input, ?> map, Class<Input> clazz) {
    if (map instanceof StatefulMapOperation) {
    }
    else return new FlameMap<>(map, clazz);
  }

  @Override
  public <Input> Graph.Vertex vertex(MapOperation<List<Input>, ?> map, int window) {
    return null;
  }

  @Override
  public void connect(Graph.Vertex source, Graph.Vertex sink) {

  }

  @Override
  public Graph build() {
    return null;
  }
}
