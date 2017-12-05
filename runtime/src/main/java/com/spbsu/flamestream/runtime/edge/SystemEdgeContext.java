package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorPath;
import akka.actor.ActorRefFactory;

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
  public String nodeId() {
    return nodeId;
  }

  @Override
  public String edgeId() {
    return edgeId;
  }

  public ActorRefFactory refFactory() {
    return refFactory;
  }
}
