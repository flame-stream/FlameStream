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

  Stream<Vertex> adjacent(Vertex vertex);

  Vertex source();

  Vertex sink();

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

    Builder link(Vertex from, Vertex to) {
      adjLists.put(from, to);
      return this;
    }

    Graph build(Source source, Sink sink) {
      if (adjLists.values().contains(source)) {
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
        public Stream<Vertex> adjacent(Vertex vertex) {
          return adjLists.get(vertex).stream();
        }

        @Override
        public Vertex source() {
          return source;
        }

        @Override
        public Vertex sink() {
          return sink;
        }
      };
    }
  }
}