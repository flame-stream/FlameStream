package com.spbsu.flamestream.runtime.edge;

import akka.actor.ActorPath;
import com.spbsu.flamestream.core.data.meta.EdgeId;

import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SystemEdgeContext that = (SystemEdgeContext) o;
    return Objects.equals(nodeId, that.nodeId) &&
            Objects.equals(edgeId, that.edgeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeId, edgeId);
  }
}
