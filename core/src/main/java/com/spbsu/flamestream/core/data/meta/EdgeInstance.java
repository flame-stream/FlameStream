package com.spbsu.flamestream.core.data.meta;

import java.util.Objects;

public class EdgeInstance implements Comparable<EdgeInstance> {
  public static final EdgeInstance MAX = new EdgeInstance("", "") {
    @Override
    public int compareTo(EdgeInstance o) {
      return 1;
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }
  };

  public static final EdgeInstance MIN = new EdgeInstance("", "") {
    @Override
    public int compareTo(EdgeInstance o) {
      return -1;
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }
  };

  private final String edgeId;
  private final String nodeId;

  public EdgeInstance(String edgeId, String nodeId) {
    this.edgeId = edgeId;
    this.nodeId = nodeId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final EdgeInstance that = (EdgeInstance) o;
    return Objects.equals(edgeId, that.edgeId) &&
            Objects.equals(nodeId, that.nodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(edgeId, nodeId);
  }

  @Override
  public int compareTo(EdgeInstance o) {
    if (edgeId.compareTo(o.edgeId) < 0) {
      return -1;
    } else if (edgeId.compareTo(o.edgeId) > 0) {
      return 1;
    } else {
      return nodeId.compareTo(o.nodeId);
    }
  }

  @Override
  public String toString() {
    return edgeId + '@' + nodeId;
  }
}
