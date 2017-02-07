package com.spbsu.datastream.core.graph;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Created by marnikitta on 2/7/17.
 */
public class Rehasher<T> implements Graph {
  private final InPort inPort = new InPort();
  private final OutPort outPort = new OutPort();

  private final Hash<T> hash;
  private final int buckets;

  public Rehasher(final Hash<T> hash, final int buckets) {
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

  @Override
  public Map<OutPort, InPort> downstreams() {
    return Collections.emptyMap();
  }

  @Override
  public Map<InPort, OutPort> upstreams() {
    return Collections.emptyMap();
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
    final Rehasher<?> rehasher = (Rehasher<?>) o;
    return buckets == rehasher.buckets &&
            Objects.equals(inPort, rehasher.inPort) &&
            Objects.equals(outPort, rehasher.outPort) &&
            Objects.equals(hash, rehasher.hash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inPort, outPort, hash, buckets);
  }

  @Override
  public String toString() {
    return "Rehasher{" + "inPort=" + inPort +
            ", outPort=" + outPort +
            ", hash=" + hash +
            ", buckets=" + buckets +
            '}';
  }
}
