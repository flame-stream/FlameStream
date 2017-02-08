package com.spbsu.datastream.core.materializer.cluster;

import com.spbsu.datastream.core.graph.FlatGraph;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.Source;
import com.spbsu.datastream.core.materializer.MaterializationException;
import com.spbsu.datastream.core.materializer.Materializer;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by marnikitta on 2/7/17.
 */
public class ClusterMaterializer implements Materializer {

  private static void assertClosed(final Graph graph) {
    if (!graph.isClosed()) {
      throw new MaterializationException("Graph should be closed");
    }
  }

  @Override
  public void materialize(final Graph graph) {
    assertClosed(graph);

    final FlatGraph flatGraph = FlatGraph.flattened(graph);

    final List<Source> sources = flatGraph.subGraphs().stream()
            .filter(Source.class::isInstance).map(Source.class::cast)
            .collect(Collectors.toList());
  }
}
