package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.MapOperation;

import java.util.List;

public interface FlameBuilder {
  <Input> Graph.Vertex vertex(MapOperation<Input, ?> map, Class<Input> clazz);
  <Input> Graph.Vertex vertex(MapOperation<List<Input>, ?> map, int window);
  void connect(Graph.Vertex source, Graph.Vertex sink);

  Graph build();
}
