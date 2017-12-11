package com.spbsu.flamestream.example;

import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.example.index.InvertedIndexCheckers;
import com.spbsu.flamestream.example.index.InvertedIndexGraph;
import com.spbsu.flamestream.example.index.model.WordBase;
import com.spbsu.flamestream.example.wordcount.WordCountCheckers;
import com.spbsu.flamestream.example.wordcount.WordCountGraph;
import com.spbsu.flamestream.example.wordcount.model.WordCounter;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.front.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.rear.akka.AkkaRearType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ExamplesTest extends FlameStreamSuite {

  @DataProvider
  public static Object[][] examples() {
    return new Object[][]{
            {
                    "WordCounter",
                    WordCountCheckers.CHECK_COUNT,
                    new WordCountGraph().get(),
                    WordCounter.class,
                    5
            },
            {
                    "SmallDump",
                    InvertedIndexCheckers.CHECK_INDEX_WITH_SMALL_DUMP,
                    new InvertedIndexGraph().get(),
                    WordBase.class,
                    5
            },
            {

                    "IndexAndRankingStorage",
                    InvertedIndexCheckers.CHECK_INDEX_AND_RANKING_STORAGE_WITH_SMALL_DUMP,
                    new InvertedIndexGraph().get(),
                    WordBase.class,
                    5
            },
            {
                    "IndexWithRanking",
                    InvertedIndexCheckers.CHECK_INDEX_WITH_RANKING,
                    new InvertedIndexGraph().get(),
                    WordBase.class,
                    60
            }

    };
  }

  @Test(dataProvider = "examples")
  public <T, R> void localEnvironmentTest(String name,
                                          ExampleChecker<T, R> checker,
                                          Graph graph,
                                          Class<R> resultClass,
                                          int awaitSeconds) throws InterruptedException {
    final LocalRuntime runtime = new LocalRuntime(4);
    System.out.println("Running example test " + name);

    final FlameRuntime.Flame flame = runtime.run(graph);
    {
      final List<R> result = Collections.synchronizedList(new ArrayList<>());
      flame.attachRear(name + "Rear", new AkkaRearType<>(runtime.system(), resultClass))
              .forEach(r -> r.addListener(result::add));

      final Consumer<T> sink = randomConsumer(
              flame.attachFront(name + "Sink", new AkkaFrontType<T>(runtime.system()))
                      .collect(Collectors.toList())
      );

      checker.input().forEach(sink);
      TimeUnit.SECONDS.sleep(awaitSeconds);
      checker.assertCorrect(result.stream());
    }
  }
}
