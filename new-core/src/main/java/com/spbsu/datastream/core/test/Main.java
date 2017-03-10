package com.spbsu.datastream.core.test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.graph.FlatGraph;
import com.spbsu.datastream.core.graph.ShardMappedGraph;
import com.spbsu.datastream.core.graph.Sink;
import com.spbsu.datastream.core.graph.Source;
import com.spbsu.datastream.core.graph.ops.ConsumerSink;
import com.spbsu.datastream.core.graph.ops.SpliteratorSource;
import com.spbsu.datastream.core.materializer.NodeImpl;
import com.spbsu.datastream.core.materializer.TickContext;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;
import com.spbsu.datastream.core.materializer.atomic.AtomicHandleImpl;
import com.spbsu.datastream.core.materializer.locator.LocalPortLocator;
import com.spbsu.datastream.core.materializer.manager.TickGraphManager;

import java.net.InetAddress;
import java.util.Spliterator;
import java.util.UUID;
import java.util.stream.Stream;

import static com.spbsu.datastream.core.materializer.manager.TickGraphManagerApi.TickStarted;

public class Main {
  public static void main(final String... args) throws Exception {
    ActorSystem system = ActorSystem.create();

    Spliterator<String> spliterator = Stream.generate(UUID::randomUUID).map(UUID::toString).limit(100).spliterator();
    Source<String> source = new SpliteratorSource<>(spliterator);
    Sink<String> sink = new ConsumerSink<>(System.out::println);
    FlatGraph graph = FlatGraph.flattened(source.fuse(sink, source.outPort(), sink.inPort()));
    ShardMappedGraph mapped = new ShardMappedGraph(graph, new NodeImpl(InetAddress.getLocalHost()));

    ActorRef manager = system.actorOf(TickGraphManager.props(new TestTickContext(), mapped), "tickManager");
    manager.tell(new TickStarted(), null);
  }

  private static class TestTickContext implements TickContext {
    private final LocalPortLocator localPortLocator = new LocalPortLocator();
    private final AtomicHandle handle = new AtomicHandleImpl(localPortLocator);

    @Override
    public LocalPortLocator localLocator() {
      return localPortLocator;
    }

    @Override
    public AtomicHandle handle() {
      return handle;
    }
  }
}
