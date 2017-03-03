package com.spbsu.datastream.core.materializer;

import akka.actor.ActorSystem;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.ShardMappedGraph;

public class AkkaMaterializer implements Materializer {
  private final ActorSystem system;

  public AkkaMaterializer(final ActorSystem system) {
    this.system = system;
  }

  @Override
  public void materialize(final ShardMappedGraph graph) {
    for (AtomicGraph atomic : graph.subGraphs()) {
    }
  }

  private void materializeAtomic(final AtomicGraph atomic) {
  }
}
