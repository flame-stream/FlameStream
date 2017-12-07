package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorPath;
import akka.actor.ActorRefFactory;
import com.spbsu.flamestream.core.data.meta.EdgeId;

public class SystemEdgeContext implements EdgeContext {
  private final ActorPath nodePath;
  private final String nodeId;
  private final ActorRefFactory refFactory;
  private final String edgeId;

  public SystemEdgeContext(ActorPath nodePath, String nodeId, String edgeId, ActorRefFactory refFactory) {
    this.nodeId = nodeId;
    this.nodePath = nodePath;
    this.refFactory = refFactory;
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

  public ActorRefFactory refFactory() {
    return refFactory;
  }
}
