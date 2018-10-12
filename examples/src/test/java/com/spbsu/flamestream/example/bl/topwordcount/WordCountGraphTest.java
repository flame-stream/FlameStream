package com.spbsu.flamestream.example.bl.topwordcount;

import com.spbsu.flamestream.example.bl.topwordcount.model.WordsTop;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

/**
 * User: Artem
 * Date: 19.12.2017
 */
public class WordCountGraphTest extends FlameAkkaSuite {
  public class FinalWordsTopConsumer implements Consumer<WordsTop> {
    private final AtomicReference<Map<String, Integer>> wordCountsReference = new AtomicReference<>(new HashMap<>());

    @Override
    public void accept(WordsTop value) {
      synchronized (wordCountsReference) {
        switch (new LexicographicComparator<Integer>().compare(
                dewordifiedTop(value.wordCounters()),
                dewordifiedTop(wordCountsReference.get())
        )) {
          case 1:
            wordCountsReference.set(value.wordCounters());
            wordCountsReference.notifyAll();
            break;
          case 0:
            break;
          case -1:
            throw new IllegalStateException("Top is decreasing");
        }
      }
    }

    public void await(long timeout, TimeUnit unit, Map<String, Integer> expectedWordCounts) throws
                                                                                            InterruptedException {
      final long stop = System.currentTimeMillis() + unit.toMillis(timeout);
      synchronized (wordCountsReference) {
        while (!dewordifiedTop(wordCountsReference.get()).equals(dewordifiedTop(expectedWordCounts).stream()
                .limit(wordCountsReference.get().size())
                .collect(Collectors.toList()))) {
          wordCountsReference.wait(stop - System.currentTimeMillis());
        }
      }
    }

    public Map<String, Integer> wordCountsReference() {
      synchronized (wordCountsReference) {
        return wordCountsReference.get();
      }
    }

    private List<Integer> dewordifiedTop(Map<String, Integer> wordCounts) {
      return wordCounts.values().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
    }
  }

  static class LexicographicComparator<T extends Comparable<T>> implements Comparator<List<T>> {
    private final Comparator<T> elementComparator;

    public LexicographicComparator(final Comparator<T> elementComparator) {
      this.elementComparator = elementComparator;
    }

    public LexicographicComparator() {
      this(new Comparator<T>() {
        @Override
        public int compare(T o1, T o2) {
          return o1.compareTo(o2);
        }
      });
    }

    @Override
    public int compare(List<T> l1, List<T> l2) {
      final Iterator<T> i1 = l1.iterator(), i2 = l2.iterator();
      while (i1.hasNext() && i2.hasNext()) {
        final int cmp = elementComparator.compare(i1.next(), i2.next());
        if (cmp != 0) {
          return cmp;
        }
      }
      if (i1.hasNext()) {
        return 1;
      }
      if (i2.hasNext()) {
        return -1;
      }
      return 0;
    }
  }

  @Test(invocationCount = 10)
  public void localEnvironmentTest() throws InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime.Builder().maxElementsInGraph(2)
            .millisBetweenCommits(500)
            .build()) {
      final FlameRuntime.Flame flame = runtime.run(new WordCountGraph().get());
      {
        final int lineSize = 50;
        final int streamSize = 10;
        final Queue<String> input = Stream.generate(() -> {
          final String[] words = {"repka", "dedka", "babka", "zhuchka", "vnuchka"};
          return new Random().ints(lineSize, 0, words.length).mapToObj(i -> words[i])
                  .collect(joining(" "));
        }).limit(streamSize).collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
        final Pattern pattern = Pattern.compile("\\s");
        final Map<String, Integer> wordCounts = input.stream()
                .map(pattern::split)
                .flatMap(Arrays::stream)
                .collect(toMap(Function.identity(), o -> 1, Integer::sum));

        final FinalWordsTopConsumer awaitConsumer = new FinalWordsTopConsumer();
        flame.attachRear("wordCountRear", new AkkaRearType<>(runtime.system(), WordsTop.class))
                .forEach(r -> r.addListener(awaitConsumer));
        final List<AkkaFront.FrontHandle<String>> handles = flame
                .attachFront("wordCountFront", new AkkaFrontType<String>(runtime.system()))
                .collect(Collectors.toList());
        applyDataToAllHandlesAsync(input, handles);
        awaitConsumer.await(30, TimeUnit.SECONDS, wordCounts);

        Assert.assertEquals(
                awaitConsumer.wordCountsReference(),
                wordCounts.entrySet()
                        .stream()
                        .filter(entry -> awaitConsumer.wordCountsReference().containsKey(entry.getKey()))
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue))
        );
      }
    }
  }
}
