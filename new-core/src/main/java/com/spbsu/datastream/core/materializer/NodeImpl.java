package com.spbsu.datastream.core.materializer;

import java.net.InetAddress;
import java.util.Objects;

public class NodeImpl implements Node {
  private final InetAddress address;

  public NodeImpl(final InetAddress address) {
    this.address = address;
  }

  @Override
  public InetAddress address() {
    return address;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final NodeImpl node = (NodeImpl) o;
    return Objects.equals(address, node.address);
  }

  @Override
  public int hashCode() {
    return Objects.hash(address);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("NodeImpl{");
    sb.append("address=").append(address);
    sb.append('}');
    return sb.toString();
  }
}
