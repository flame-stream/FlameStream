package com.spbsu.flamestream.example.wordcount;

import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.example.AbstractExampleTest;
import com.spbsu.flamestream.example.FlameStreamExample;
import com.spbsu.flamestream.example.FlamesStreamTestGraphs;
import com.spbsu.flamestream.runtime.TheGraph;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collection;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public class WordCountTest extends AbstractExampleTest {

  @DataProvider
  public Object[][] testWordCountProvider() {
    return new Object[][]{
            {1, 4},
            {4, 4},
            {4, 1},
            {1, 1}
    };
  }

  @Test(dataProvider = "testWordCountProvider")
  public void testWordCount(int fronts, int workers) {
    test(WordCountCheckers.CHECK_COUNT, fronts, workers, 20, 20);
  }

  @Override
  protected TheGraph graph(Collection<Integer> fronts, AtomicGraph sink) {
    return FlamesStreamTestGraphs.createTheGraph(FlameStreamExample.WORD_COUNT, fronts, sink);
  }
}
