package com.spbsu.flamestream.runtime.graph.materialization;

import com.spbsu.flamestream.core.Graph;

/**
 * User: Artem
 * Date: 28.11.2017
 */
public interface GraphMaterializer {
  GraphMaterialization materialize(Graph graph);
}
