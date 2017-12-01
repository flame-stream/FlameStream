package com.spbsu.flamestream.core;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

public interface Graph {
  Stream<Vertex> nodes();

  Stream<Vertex> outputs(Vertex vertex);

  Stream<Vertex> inputs(Vertex vertex);

  boolean isBroadcast(Vertex vertex);

  Source source();

  Sink sink();

  interface Vertex {
    String id();

    abstract class Stub implements Vertex {
      private final long id = ThreadLocalRandom.current().nextLong();

      @Override
      public String id() {
        return toString() + "{id=" + id + "}";
      }
    }

    abstract class LocalTimeStub extends Stub {
      private int localTime = 0;

      protected final int incrementLocalTimeAndGet() {
        this.localTime += 1;
        return localTime;
      }
    }
  }

  class Builder {
    private Multimap<Vertex, Vertex> adjLists = HashMultimap.create();
    private Multimap<Vertex, Vertex> invertedAdjLists = HashMultimap.create();

    public Builder link(Vertex from, Vertex to) {
      adjLists.put(from, to);
      invertedAdjLists.put(to, from);
      return this;
    }

    public Graph build(Source source, Sink sink) {
      if (invertedAdjLists.keySet().contains(source)) {
        throw new IllegalStateException("Source must not have inputs");
      } else if (adjLists.keySet().contains(sink)) {
        throw new IllegalStateException("Source must not have outputs");
      }

      final Collection<Vertex> allVertices = new ArrayList<>(adjLists.keySet());
      allVertices.add(sink);
      return new Graph() {
        @Override
        public Stream<Vertex> nodes() {
          return allVertices.stream();
        }

        @Override
        public Stream<Vertex> outputs(Vertex vertex) {
          return adjLists.get(vertex).stream();
        }

        @Override
        public Stream<Vertex> inputs(Vertex vertex) {
          return invertedAdjLists.get(vertex).stream();
        }

        @Override
        public boolean isBroadcast(Vertex vertex) {
          return adjLists.get(vertex).size() > 1;
        }

        @Override
        public Source source() {
          return source;
        }

        @Override
        public Sink sink() {
          return sink;
        }
      };
    }
  }
}