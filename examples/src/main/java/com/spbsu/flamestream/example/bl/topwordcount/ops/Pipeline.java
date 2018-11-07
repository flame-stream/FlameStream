package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.Graph;

public interface Pipeline {
  Graph.Vertex in();

  Graph.Vertex out();
}
