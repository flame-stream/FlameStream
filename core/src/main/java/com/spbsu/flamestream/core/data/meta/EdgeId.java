package com.spbsu.flamestream.core.data.meta;

import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.Objects;

public final class EdgeId implements Comparable<EdgeId> {
  public enum Limit {
    Min,
    None,
    Max
  }

  public static final EdgeId MIN = new EdgeId(Limit.Min, "", "");
  public static final EdgeId MAX = new EdgeId(Limit.Max, "", "");

  private static final Comparator<EdgeId> COMPARATOR = Comparator
          .<EdgeId, Limit>comparing(edgeId -> edgeId.limit)
          .thenComparing(EdgeId::edgeName)
          .thenComparing(EdgeId::nodeId);

  private final Limit limit;
  private final String edgeName;
  private final String nodeId;

  private EdgeId(Limit limit, String edgeName, String nodeId) {
    this.limit = limit;
    this.edgeName = edgeName;
    this.nodeId = nodeId;
  }

  public EdgeId(String edgeName, String nodeId) {
    this(Limit.None, edgeName, nodeId);
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
    if (o instanceof EdgeId) {
      return compareTo((EdgeId) o) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(limit, edgeName, nodeId);
  }

  @Override
  public int compareTo(@NotNull EdgeId edgeId) {
    return COMPARATOR.compare(this, edgeId);
  }

  @Override
  public String toString() {
    return limit == Limit.None ? edgeName + "@" + nodeId : limit.toString();
  }
}
