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
  Stream<Vertex> vertices();

  Stream<Vertex> adjacent(Vertex vertex);

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
  }

  class Builder {
    private final Multimap<Vertex, Vertex> adjLists = HashMultimap.create();
    private final Multimap<Vertex, Vertex> invertedAdjLists = HashMultimap.create();

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

      return new MyGraph(adjLists, source, sink);
    }

    public static class MyGraph implements Graph {
      private final Collection<Vertex> allVertices;
      private final Source source;
      private final Sink sink;
      private final Multimap<Vertex, Vertex> adjLists;

      public MyGraph(Multimap<Vertex, Vertex> adjLists, Source source, Sink sink) {
        this.allVertices = new ArrayList<>(adjLists.keySet());
        allVertices.add(sink);
        this.adjLists = HashMultimap.create(adjLists);
        this.source = source;
        this.sink = sink;
      }

      @Override
      public Stream<Vertex> vertices() {
        return allVertices.stream();
      }

      @Override
      public Stream<Vertex> adjacent(Vertex vertex) {
        return adjLists.get(vertex).stream();
      }

      @Override
      public Source source() {
        return source;
      }

      @Override
      public Sink sink() {
        return sink;
      }
    }
  }
}