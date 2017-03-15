package com.spbsu.datastream.core.test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.HashableString;
import com.spbsu.datastream.core.graph.*;
import com.spbsu.datastream.core.graph.ops.ConsumerSink;
import com.spbsu.datastream.core.graph.ops.SpliteratorSource;
import com.spbsu.datastream.core.materializer.manager.TickGraphManager;
import com.spbsu.datastream.core.materializer.manager.TickGraphManagerApi;

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
    TheGraph theGraph = new TheGraph(graph);

    ActorRef manager = system.actorOf(
            TickGraphManager.props(system.deadLetters(),
                    new HashRange(Integer.MIN_VALUE, Integer.MAX_VALUE),
                    theGraph),
            "manager");
    manager.tell(new TickGraphManagerApi.TickStarted(), null);
  }
}
