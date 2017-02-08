package com.spbsu.datastream.core.graph.impl;

import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.LogicalGraph;
import com.spbsu.datastream.core.graph.OutPort;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Created by marnikitta on 2/7/17.
 */
public final class Rehash<T> implements LogicalGraph {
  private final InPort inPort = new InPort();
  private final OutPort outPort = new OutPort();

  private final Hash<T> hash;
  private final int buckets;

  public Rehash(final Hash<T> hash, final int buckets) {
    this.hash = hash;
    this.buckets = buckets;
  }

  @Override
  public Set<InPort> inPorts() {
    return Collections.singleton(inPort);
  }

  @Override
  public Set<OutPort> outPorts() {
    return Collections.singleton(outPort);
  }

  public Hash<T> grouping() {
    return hash;
  }

  public int buckets() {
    return buckets;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final Rehash<?> rehash = (Rehash<?>) o;
    return buckets == rehash.buckets &&
            Objects.equals(inPort, rehash.inPort) &&
            Objects.equals(outPort, rehash.outPort) &&
            Objects.equals(hash, rehash.hash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inPort, outPort, hash, buckets);
  }

  @Override
  public String toString() {
    return "Rehash{" + "inPort=" + inPort +
            ", outPort=" + outPort +
            ", hash=" + hash +
            ", buckets=" + buckets +
            '}';
  }
}
