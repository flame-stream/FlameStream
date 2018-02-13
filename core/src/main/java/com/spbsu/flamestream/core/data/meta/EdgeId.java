package com.spbsu.flamestream.core.data.meta;

import java.util.Objects;

public class EdgeId implements Comparable<EdgeId> {
  static final EdgeId MAX = new EdgeId("max", "max") {
    @Override
    public int compareTo(EdgeId o) {
      return 1;
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }
  };

  public static final EdgeId MIN = new EdgeId("min", "min") {
    @Override
    public int compareTo(EdgeId o) {
      return -1;
    }

    @Override
    public boolean equals(Object o) {
      return false;
    }
  };

  private final String edgeName;
  private final String nodeId;

  public EdgeId(String edgeName, String nodeId) {
    this.edgeName = edgeName;
    this.nodeId = nodeId;
  }

  public String edgeName() {
    return edgeName;
  }

  public String nodeId() {
    return nodeId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final EdgeId that = (EdgeId) o;
    return Objects.equals(edgeName, that.edgeName) &&
            Objects.equals(nodeId, that.nodeId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(edgeName, nodeId);
  }

  @Override
  public int compareTo(EdgeId o) {
    if (edgeName.compareTo(o.edgeName) < 0) {
      return -1;
    } else if (edgeName.compareTo(o.edgeName) > 0) {
      return 1;
    } else {
      return nodeId.compareTo(o.nodeId);
    }
  }

  @Override
  public String toString() {
    return edgeName + '@' + nodeId;
  }
}
