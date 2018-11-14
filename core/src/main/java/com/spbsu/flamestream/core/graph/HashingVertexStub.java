package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.HashFunction;
import org.jetbrains.annotations.Nullable;

public abstract class HashingVertexStub extends Graph.Vertex.Stub {
  @Nullable
  public HashFunction hash() {
    return null;
  }
}
