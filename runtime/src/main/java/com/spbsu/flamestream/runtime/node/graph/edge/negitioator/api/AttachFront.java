package com.spbsu.flamestream.runtime.node.graph.edge.negitioator.api;

public class AttachFront {
  private final String frontId;
  private final String graphId;

  public AttachFront(String frontId, String graphId) {
    this.frontId = frontId;
    this.graphId = graphId;
  }

  public String frontId() {
    return frontId;
  }

  public String graphId() {
    return graphId;
  }
}
