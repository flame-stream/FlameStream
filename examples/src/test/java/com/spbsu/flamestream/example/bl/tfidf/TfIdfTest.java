package com.spbsu.flamestream.example.bl.tfidf;

import akka.japi.Pair;
import com.spbsu.flamestream.example.bl.tfidf.model.TextDocument;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.DocCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordCounter;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordDocCounter;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

/**
 * User: Artem
 * Date: 19.12.2017
 */

public class TfIdfTest extends FlameAkkaSuite {
  private static class Tale {
    String name;
    String words[];

    Tale(String name, String words[]) {
      this.name = name;
      this.words = words;
    }
  }

  private static Tale[] tales = {
          new Tale("Repka",
            new String[]{"posadil", "ded", "repku", "i", "vysrosla", "repka", "bolshaya-prebolshaya"}),
          new Tale("Fisher and fish",
                    new String[]{"zhili-byli", "starik", "so", "staruhoy", "u", "samogo", "sinego", "morya"}),
  };

 private Stream<Pair<String, String>> wordDocPairs(Queue<TextDocument> queue) {
     final Pattern pattern = Pattern.compile("\\s");
     return queue.stream()
             .flatMap(doc -> Arrays.stream(pattern.split(doc.content()))
                     .map(word -> new Pair<>(word, doc.name())));
 }

 private int sumValues(Map<String, Integer> map) {
     return map.values().stream().reduce(Integer::sum).orElse(0);
 }

 private void testTales(int streamSize, int lineSize, List<Tale> tales) throws InterruptedException {
      try (final LocalRuntime runtime = new LocalRuntime.Builder().maxElementsInGraph(2)
              .millisBetweenCommits(500)
              .build()) {
          final FlameRuntime.Flame flame = runtime.run(new TfIdfGraph().get());
          {
              final Queue<TextDocument> input = Stream.generate(() ->
                      tales.stream().map(tale -> {
                          String content = new Random().ints(lineSize, 0, tale.words.length)
                                  .mapToObj(i -> tale.words[i])
                                  .collect(joining(" "));
                          return new TextDocument(tale.name, content); }))
                      .limit(streamSize).flatMap(Function.identity())
                      .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));

              final Pattern pattern = Pattern.compile("\\s");

              final Map<String, Map<String, Integer>> expectedWordDoc = wordDocPairs(input)
                      .collect(groupingBy(Pair::first, mapping(Pair::second, toMap(Function.identity(), o -> 1, Integer::sum))));
              final Map<String, Integer> expectedDoc = input.stream()
                      .flatMap(doc -> Arrays.stream(pattern.split(doc.content()))
                              .map(word -> new Pair<>(doc.name(), word)))
                      .collect(toMap(Pair::first, o -> 1, Integer::sum));
              final Map<String, Integer> expectedIdf = wordDocPairs(input)
                      .collect(groupingBy(Pair::first, collectingAndThen(toSet(), Set::size)));

              int nExpected = Stream.concat(expectedWordDoc.values().stream(), Stream.of(expectedDoc, expectedIdf)).map(e -> sumValues(e)).reduce(Integer::sum).orElse(0);

              final AwaitResultConsumer<Object> awaitConsumer = new AwaitResultConsumer<>(nExpected);
              flame.attachRear("tfidfRear", new AkkaRearType<>(runtime.system(), Object.class))
                      .forEach(r -> r.addListener(awaitConsumer));
              final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
                      .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(runtime.system()))
                      .collect(toList());
              applyDataToAllHandlesAsync(input, handles);
              awaitConsumer.await(5, TimeUnit.MINUTES);

              final Map<String, Integer> actualDoc = new HashMap<>();
              final Map<String, Integer> actualIdf = new HashMap<>();
              final Map<Pair<String, String>, Integer> actualPairs = new HashMap<>();
              awaitConsumer.result().forEach(o -> {
                  if (o instanceof WordDocCounter){
                      final WordDocCounter wordDocContainer = (WordDocCounter) o;
                      Pair<String, String> p = new Pair(wordDocContainer.word(), wordDocContainer.document());
                      actualPairs.putIfAbsent(p, 0);
                      actualPairs.computeIfPresent(p, (uid, old) -> Math.max(wordDocContainer.count(), old));
                  } else if (o instanceof DocCounter) {
                      final DocCounter docContainer = (DocCounter) o;
                      actualDoc.putIfAbsent(docContainer.document(), 0);
                      actualDoc.computeIfPresent(docContainer.document(), (uid, old) -> Math.max(docContainer.count(), old));
                  } else if (o instanceof WordCounter) {
                      final WordCounter wordContainer = (WordCounter) o;
                      actualIdf.putIfAbsent(wordContainer.word(), 0);
                      actualIdf.computeIfPresent(wordContainer.word(), (uid, old) -> Math.max(wordContainer.count(), old));
                  } else {
                      System.out.println("unexpected: " + o);
                  }
              });

              final Map<String, Map<String, Integer>> actualWordDoc = actualPairs.entrySet().stream()
                      .collect(groupingBy(v -> v.getKey().first(),
                              toMap(v -> v.getKey().second(), v -> v.getValue())));

              Assert.assertEquals(actualWordDoc, expectedWordDoc);
              Assert.assertEquals(actualDoc, expectedDoc);
              Assert.assertEquals(actualIdf, expectedIdf);
            }
        }
    }

  private void testSingleTale(int streamSize, int lineSize, Tale tale) throws InterruptedException {
      testTales(2, 10, Arrays.asList(tale));
  }


  @Test(invocationCount = 1)
  public void singleDocumentTest() throws InterruptedException {
    for (Tale tale: tales) {
      testSingleTale(1, 50, tale);
    }
  }


  public void singleDocumentManyTimesTest() throws InterruptedException {
    for (Tale tale: tales) {
      testSingleTale(100, 50, tale);
    }
  }

  @Test(invocationCount = 1)
  public void twoIntersectingDocumentsTest() throws InterruptedException {
      Tale tale1 = new Tale("t1", new String[]{"word1", "word2"});
      Tale tale2 = new Tale("t2", new String[]{"word2", "word3"});
      testTales(2, 10, Arrays.asList(tale1, tale2));
  }
}
