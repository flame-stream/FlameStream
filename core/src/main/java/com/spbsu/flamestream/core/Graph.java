package com.spbsu.flamestream.core;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import org.jooq.lambda.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
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
        // id shouldn't contain JVM-instance specific fields, e.g. hashCode
        return String.valueOf(id);
      }
    }
  }

  class Builder {
    private final Multimap<Vertex, Vertex> adjLists = HashMultimap.create();
    private final Multimap<Vertex, Vertex> invertedAdjLists = HashMultimap.create();
    private final List<Tuple2<Vertex, Vertex>> asyncs = new ArrayList<>();

    public Builder linkAsync(Vertex from, Vertex to) {
      adjLists.put(from, to);
      invertedAdjLists.put(to, from);
      asyncs.add(new Tuple2<>(from, to));
      return this;
    }

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

      return new MyGraph(adjLists, source, sink, asyncs);
    }

    public static class MyGraph implements Graph {
      private final List<Vertex> allVertices;
      private final Source source;
      private final Sink sink;
      private final Map<Vertex, Collection<Vertex>> adjLists;
      private final List<Tuple2<Vertex, Vertex>> asyncs;

      public MyGraph(Multimap<Vertex, Vertex> adjLists,
                     Source source,
                     Sink sink,
                     List<Tuple2<Vertex, Vertex>> asyncs) {
        this.asyncs = asyncs;
        this.allVertices = new ArrayList<>(adjLists.keySet());
        allVertices.add(sink);
        // Multimap is converted to Map<.,Set> to support serialization
        this.adjLists = adjLists.entries().stream().collect(Collectors.groupingBy(
                Map.Entry::getKey,
                Collectors.mapping(
                        Map.Entry::getValue,
                        Collectors.toCollection(HashSet::new)
                )
        ));

        this.source = source;
        this.sink = sink;
      }

      public boolean isAsync(Vertex from, Vertex to) {
        return asyncs.contains(new Tuple2<>(from, to));
      }

      @Override
      public Stream<Vertex> vertices() {
        return allVertices.stream();
      }

      @Override
      public Stream<Vertex> adjacent(Vertex vertex) {
        return adjLists.getOrDefault(vertex, Collections.emptyList()).stream();
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