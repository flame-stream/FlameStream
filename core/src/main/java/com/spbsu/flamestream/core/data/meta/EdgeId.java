package com.spbsu.flamestream.core.data.meta;

import java.util.Objects;

public class EdgeId implements Comparable<EdgeId> {
  static public class Max extends EdgeId {
    public static EdgeId INSTANCE = new Max();

    private Max() {
      super("max", "max");
    }

    @Override
    public int compareTo(EdgeId o) {
      if (o == INSTANCE) {
        return 0;
      }
      return 1;
    }

    @Override
    public boolean equals(Object o) {
      return o == INSTANCE;
    }
  }

  static public class Min extends EdgeId {
    public static EdgeId INSTANCE = new Min();

    private Min() {
      super("min", "min");
    }
    @Override
    public int compareTo(EdgeId o) {
      if (o == INSTANCE) {
        return 0;
      }
      return -1;
    }

    @Override
    public boolean equals(Object o) {
      return o == INSTANCE;
    }
  }

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
    if (o == Min.INSTANCE) {
      return 1;
    } else if (o == Max.INSTANCE) {
      return -1;
    }

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
