package com.spbsu.flamestream.example.inverted_index;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.core.graph.barrier.BarrierSink;
import com.spbsu.flamestream.example.AbstractExampleTest;
import com.spbsu.flamestream.example.FlameStreamExamples;
import com.spbsu.flamestream.runtime.TheGraph;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class InvertedIndexTest extends AbstractExampleTest {

  @Test
  public void testIndexWithSmallDump() {
    test(InvertedIndexCheckers.CHECK_INDEX_WITH_SMALL_DUMP, 2, 2, 20, 25);
  }

  @Test
  public void testIndexAndRankingStorageWithSmallDump() {
    test(InvertedIndexCheckers.CHECK_INDEX_AND_RANKING_STORAGE_WITH_SMALL_DUMP, 4, 4, 60, 25);
  }

  @Test
  public void testIndexWithRanking() {
    test(InvertedIndexCheckers.CHECK_INDEX_WITH_RANKING, 4, 4, 90, 25);
  }

  @Override
  protected TheGraph graph(Collection<Integer> fronts, AtomicGraph sink) {
    final BarrierSink barrierSink = new BarrierSink(sink);
    final Graph graph = FlameStreamExamples.INVERTED_INDEX.graph(barrierSink);

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> graph.inPorts().get(0)));
    return new TheGraph(graph, frontBindings);
  }
}
