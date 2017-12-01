package com.spbsu.flamestream.runtime.graph.materialization;

import com.spbsu.flamestream.runtime.graph.materialization.vertices.VertexJoba;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 01.12.2017
 */
public interface GraphMaterialization {
  VertexJoba jobaForVertex(String vertexId);

  void forEachJoba(Consumer<VertexJoba> consumer);
}
