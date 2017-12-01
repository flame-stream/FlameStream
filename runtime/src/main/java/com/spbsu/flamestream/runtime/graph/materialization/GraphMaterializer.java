package com.spbsu.flamestream.runtime.graph.materialization;

import com.spbsu.flamestream.runtime.graph.materialization.vertices.VertexJoba;

import java.util.Map;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public interface GraphMaterializer {
  Map<String, VertexJoba> materialize();
}
