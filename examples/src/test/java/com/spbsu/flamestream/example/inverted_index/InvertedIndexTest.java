package com.spbsu.flamestream.example.inverted_index;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.barrier.BarrierSink;
import com.spbsu.flamestream.example.ExampleChecker;
import com.spbsu.flamestream.example.FlameStreamExamples;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordContainer;
import com.spbsu.flamestream.runtime.RemoteTestStand;
import com.spbsu.flamestream.runtime.TheGraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class InvertedIndexTest {


  private static void testIndex(ExampleChecker<WikipediaPage, WordContainer> checker) {
    try (RemoteTestStand stage = new RemoteTestStand(4)) {
      final List<WordContainer> result = new ArrayList<>();
      stage.deploy(invertedIndexGraph(
              stage.environment().availableFronts(),
              stage.environment().wrapInSink(o -> result.add((WordContainer) o))
      ), 20, 1);

      final Consumer<Object> sink = stage.randomFrontConsumer(4);
      checker.input().forEach(sink);

      stage.awaitTick(25);
      checker.check(result.stream());
    }
  }

  private static TheGraph invertedIndexGraph(Collection<Integer> fronts, AtomicGraph sink) {
    final BarrierSink barrierSink = new BarrierSink(sink);
    final Graph graph = FlameStreamExamples.INVERTED_INDEX.graph(barrierSink);

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> graph.inPorts().get(0)));
    return new TheGraph(graph, frontBindings);
  }
}
