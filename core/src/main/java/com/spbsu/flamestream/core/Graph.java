package com.spbsu.flamestream.core;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.spbsu.flamestream.core.graph.WiringException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;

public interface Graph<In, Out> {
  Stream<Node<?>> nodes();

  Stream<Node<?>> outputs(Node<?> node);

  Node<In> source();

  Node<Out> sink();

  interface Node<I> {
    HashFunction<? super I> inputHash();

    abstract class Stub<I> implements Node<I> {
      private int localTime = 0;

      protected final int incrementLocalTimeAndGet() {
        this.localTime += 1;
        return localTime;
      }
    }
  }

  class Builder {
    private Multimap<Node, Node> adjLists = HashMultimap.create();

    Builder link(Node from, Node to) {
      adjLists.put(from, to);
      return this;
    }

    <I, O> Graph build(Node<I> source, Node<O> sink) {
      if (adjLists.values().contains(source)) {
        throw new WiringException("Source must not have inputs");
      } else if (adjLists.keySet().contains(sink)) {
        throw new WiringException("Source must not have outputs");
      }

      final Collection<Node> allNodes = new ArrayList<>(adjLists.keySet());
      allNodes.add(sink);
      return new Graph() {
        @Override
        public Stream<Node> nodes() {
          return allNodes.stream();
        }

        @Override
        public Stream<Node> outputs(Node node) {
          return adjLists.get(node).stream();
        }

        @Override
        public Node<I> source() {
          return source;
        }

        @Override
        public Node<O> sink() {
          return sink;
        }
      };
    }
  }
}