package com.spbsu.flamestream.core;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

public interface Graph<In, Out> {
  Stream<Vertex<?>> nodes();

  Stream<Vertex<?>> outputs(Vertex<?> vertex);

  Vertex<In> source();

  Vertex<Out> sink();

  interface Vertex<I> {
    HashFunction<? super I> inputHash();

    String id();

    abstract class Stub<I> implements Vertex<I> {
      private final long id = ThreadLocalRandom.current().nextLong();
      private int localTime = 0;

      protected final int incrementLocalTimeAndGet() {
        this.localTime += 1;
        return localTime;
      }

      @Override
      public String id() {
        return toString() + "{id=" + id + "}";
      }
    }
  }

  class Builder {
    private Multimap<Vertex, Vertex> adjLists = HashMultimap.create();

    Builder link(Vertex from, Vertex to) {
      adjLists.put(from, to);
      return this;
    }

    <I, O> Graph build(Vertex<I> source, Vertex<O> sink) {
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
        public Stream<Vertex> outputs(Vertex vertex) {
          return adjLists.get(vertex).stream();
        }

        @Override
        public Vertex<I> source() {
          return source;
        }

        @Override
        public Vertex<O> sink() {
          return sink;
        }
      };
    }
  }
}