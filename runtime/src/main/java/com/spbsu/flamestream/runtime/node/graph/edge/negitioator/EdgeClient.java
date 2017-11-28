package com.spbsu.flamestream.runtime.node.graph.edge.negitioator;

import com.spbsu.flamestream.runtime.node.graph.edge.api.FrontInstance;
import com.spbsu.flamestream.runtime.node.graph.edge.api.RearInstance;

import java.util.List;
import java.util.function.Consumer;

public interface EdgeClient {
  List<String> frontIds(Consumer<List<String>> watcher);
  List<String> rearIds(Consumer<List<String>> watcher);

  FrontInstance<?> frontById(String frontId);
  RearInstance<?> rearById(String rearId);
}
