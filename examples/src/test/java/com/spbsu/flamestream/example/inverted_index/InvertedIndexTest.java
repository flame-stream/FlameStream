package com.spbsu.flamestream.example.inverted_index;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.example.AbstractExampleTest;
import com.spbsu.flamestream.example.FlameStreamExample;
import com.spbsu.flamestream.example.FlamesStreamTestGraphs;
import com.spbsu.flamestream.runtime.TheGraph;
import org.testng.annotations.Test;

import java.util.Collection;

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
    return FlamesStreamTestGraphs.createTheGraph(FlameStreamExample.INVERTED_INDEX, fronts, sink);
  }
}
