package com.spbsu.flamestream.example.benchmark;

import com.spbsu.flamestream.example.bl.index.InvertedIndexGraph;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.bl.index.utils.WikipeadiaInput;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;

import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 15.12.2017
 */
public class InvertedIndexBenchmark {
  public static void main(String[] args) throws InterruptedException {
    final int parallelism = 4;
    try (final LocalRuntime runtime = new LocalRuntime(parallelism, 1)) {
      final FlameRuntime.Flame flame = runtime.run(new InvertedIndexGraph().get());
      final ConcurrentSkipListMap<Integer, LatencyMeasurer> latencies = new ConcurrentSkipListMap<>();
      final AwaitCountConsumer awaitConsumer = new AwaitCountConsumer(65813);
      flame.attachRear("Rear", new AkkaRearType<>(runtime.system(), WordBase.class))
              .forEach(r -> r.addListener(wordBase -> {
                awaitConsumer.accept(wordBase);
                if (wordBase instanceof WordIndexAdd) {
                  final WordIndexAdd indexAdd = (WordIndexAdd) wordBase;
                  final int docId = IndexItemInLong.pageId(indexAdd.positions()[0]);
                  latencies.get(docId).finish();
                }
              }));

      final List<AkkaFrontType.Handle<WikipediaPage>> handles = flame
              .attachFront("Front", new AkkaFrontType<WikipediaPage>(runtime.system(), true))
              .collect(Collectors.toList());
      final AkkaFrontType.Handle<WikipediaPage> sink = handles.get(0);
      for (int i = 1; i < parallelism; i++) {
        handles.get(i).eos();
      }

      final Stream<WikipediaPage> source = WikipeadiaInput.dumpStreamFromResources(
              "wikipedia/national_football_teams_dump.xml")
              .peek(wikipediaPage -> latencies.put(wikipediaPage.id(), new LatencyMeasurer()));
      source.forEach(sink);
      sink.eos();
      awaitConsumer.await(5, TimeUnit.MINUTES);

      final LongSummaryStatistics result = new LongSummaryStatistics();
      final StringBuilder stringBuilder = new StringBuilder();
      latencies.values().stream().skip(50).forEach(latencyMeasurer -> {
        result.accept(latencyMeasurer.statistics().getMax());
        stringBuilder.append(latencyMeasurer.statistics().getMax()).append(", ");
      });
      stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length()); //remove last ", "

      System.out.println("Latencies dump: " + stringBuilder);
      System.out.println("Result: " + result);
    }
  }
}
