package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorPath;
import com.spbsu.flamestream.core.data.meta.EdgeId;

public class SystemEdgeContext implements EdgeContext {
  private final ActorPath nodePath;
  private final String nodeId;
  private final String edgeId;

  public SystemEdgeContext(ActorPath nodePath, String nodeId, String edgeId) {
    this.nodeId = nodeId;
    this.nodePath = nodePath;
    this.edgeId = edgeId;
  }

  @Override
  public ActorPath nodePath() {
    return nodePath;
  }

  @Override
  public EdgeId edgeId() {
    return new EdgeId(edgeId, nodeId);
  }
}
