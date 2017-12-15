package com.spbsu.flamestream.example.index;

import com.spbsu.flamestream.example.index.model.WikipediaPage;
import com.spbsu.flamestream.example.index.model.WordBase;
import com.spbsu.flamestream.example.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.index.utils.WikipeadiaInput;
import com.spbsu.flamestream.example.utils.LatencyMeasurer;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;

import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 15.12.2017
 */
public class InvertedIndexBenchmark {
  public static void main(String[] args) throws InterruptedException {
    try (LocalRuntime runtime = new LocalRuntime(1)) {
      final FlameRuntime.Flame flame = runtime.run(new InvertedIndexGraph().get());
      {
        final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(0, 0);

        flame.attachRear("Rear", new AkkaRearType<>(runtime.system(), WordBase.class))
                .forEach(r -> r.addListener(wordBase -> {
                  if (wordBase instanceof WordIndexAdd) {
                    final WordIndexAdd indexAdd = (WordIndexAdd) wordBase;
                    final int docId = IndexItemInLong.pageId(indexAdd.positions()[0]);
                    latencyMeasurer.finish(docId);
                  }
                }));

        final List<Consumer<WikipediaPage>> fronts = flame
                .attachFront("Front", new AkkaFrontType<WikipediaPage>(runtime.system()))
                .collect(Collectors.toList());
        final Consumer<WikipediaPage> randomConsumer = wikipediaPage -> fronts
                .get(ThreadLocalRandom.current().nextInt(fronts.size())).accept(wikipediaPage);

        final Stream<WikipediaPage> source = WikipeadiaInput.dumpStreamFromResources(
                "wikipedia/national_football_teams_dump.xml")
                .peek(wikipediaPage -> latencyMeasurer.start(wikipediaPage.id()));
        source.forEach(wikipediaPage -> {
          randomConsumer.accept(wikipediaPage);
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
        TimeUnit.SECONDS.sleep(5);

        final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
        System.out.println("Result: " + stat);
      }
    }
  }
}
