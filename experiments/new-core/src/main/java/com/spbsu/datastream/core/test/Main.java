package com.spbsu.datastream.core.test;

import akka.actor.ActorSystem;
import com.spbsu.datastream.core.graph.FlatGraph;
import com.spbsu.datastream.core.graph.ShardMappedGraph;
import com.spbsu.datastream.core.graph.Sink;
import com.spbsu.datastream.core.graph.Source;
import com.spbsu.datastream.core.graph.ops.ConsumerSink;
import com.spbsu.datastream.core.graph.ops.SpliteratorSource;
import com.spbsu.datastream.core.materializer.LocalPortLocator;
import com.spbsu.datastream.core.materializer.RemotePortLocator;
import com.spbsu.datastream.core.materializer.TickContext;
import com.spbsu.datastream.core.materializer.TickGraphManager;

import java.util.Spliterator;
import java.util.UUID;
import java.util.stream.Stream;

public class Main {
  public static void main(final String... args) throws Exception {
    ActorSystem system = ActorSystem.create();

    Spliterator<String> spliterator = Stream.generate(UUID::randomUUID).map(UUID::toString).limit(100).spliterator();
    Source<String> source = new SpliteratorSource<>(spliterator);
    Sink<String> sink = new ConsumerSink<>(System.out::println);
    FlatGraph graph = FlatGraph.flattened(source.fuse(sink, source.outPort(), sink.inPort()));
    ShardMappedGraph mapped = new ShardMappedGraph(graph, "abacaba");

    system.actorOf(TickGraphManager.props(new TestTickContext(), mapped), "tickManager");
  }

  private static class TestTickContext implements TickContext {
    private final LocalPortLocator localPortLocator = new LocalPortLocator();

    @Override
    public LocalPortLocator localLocator() {
      return localPortLocator;
    }

    @Override
    public RemotePortLocator remoteLocator() {
      throw new UnsupportedOperationException();
    }
  }
}
