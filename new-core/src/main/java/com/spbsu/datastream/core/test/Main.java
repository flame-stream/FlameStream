package com.spbsu.datastream.core.test;

import akka.actor.ActorSystem;
import com.spbsu.datastream.core.Hashable;
import com.spbsu.datastream.core.graph.*;
import com.spbsu.datastream.core.graph.ops.ConsumerSink;
import com.spbsu.datastream.core.graph.ops.SpliteratorSource;

import java.util.Spliterator;
import java.util.UUID;
import java.util.stream.Stream;

public class Main {
  public static void main(final String... args) throws Exception {
    ActorSystem system = ActorSystem.create();

    Spliterator<HashableString> spliterator = Stream.generate(UUID::randomUUID).map(UUID::toString)
            .map(HashableString::new)
            .limit(100).spliterator();
    Source<HashableString> source = new SpliteratorSource<>(spliterator);

    Sink<HashableString> sink = new ConsumerSink<>(System.out::println);

    Graph gr = source.fuse(sink, source.outPort(), sink.inPort());

    FlatGraph graph = FlatGraph.flattened(gr);
    TheGraph mapped = new TheGraph(graph);
  }

  private static class HashableString implements Hashable<HashableString> {
    private final String value;

    private HashableString(final String value) {
      this.value = value;
    }

    @Override
    public int hash() {
      return value.hashCode();
    }

    @Override
    public boolean hashEquals(final HashableString that) {
      return this.value().equals(that.value());
    }

    public String value() {
      return value;
    }
  }
}
