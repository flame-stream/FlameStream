package com.spbsu.datastream.core.wordcount;

import akka.actor.ActorPath;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.LocalCluster;
import com.spbsu.datastream.core.TestStand;
import com.spbsu.datastream.core.barrier.PreSinkMetaFilter;
import com.spbsu.datastream.core.barrier.RemoteActorConsumer;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.Broadcast;
import com.spbsu.datastream.core.graph.ops.Filter;
import com.spbsu.datastream.core.graph.ops.FlatMap;
import com.spbsu.datastream.core.graph.ops.Grouping;
import com.spbsu.datastream.core.graph.ops.Merge;
import com.spbsu.datastream.core.graph.ops.StatelessMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;

/**
 * User: Artem
 * Date: 19.06.2017
 */
public class WordCountTest {
  private static final HashFunction<WordContainer> WORD_HASH = new HashFunction<WordContainer>() {
    @Override
    public int hash(WordContainer value) {
      return HashFunction.jenkinsHash(value.word().hashCode());
    }
  };

  private static final BiPredicate<WordContainer, WordContainer> EQUALZ = new BiPredicate<WordContainer, WordContainer>() {
    @Override
    public boolean test(WordContainer o1, WordContainer o2) {
      return o1.word().equals(o2.word());
    }
  };

  private static final HashFunction<List<WordContainer>> GROUP_HASH = new HashFunction<List<WordContainer>>() {
    @Override
    public int hash(List<WordContainer> value) {
      return WORD_HASH.hash(value.get(0));
    }
  };

  @Test
  public void testSingleFront() throws InterruptedException {
    this.test(1);
  }

  @Test
  public void testMultipleFronts() throws InterruptedException {
    this.test(4);
  }

  private void test(int fronts) {
    try (LocalCluster cluster = new LocalCluster(4, fronts);
            TestStand stage = new TestStand(cluster)) {
      final Map<String, Integer> actual = new HashMap<>();
      stage.deploy(wordCountGraph(stage.frontIds(), stage.wrap(o -> {
        final WordCounter wordContainer = (WordCounter) o;
        actual.putIfAbsent(wordContainer.word(), 0);
        actual.computeIfPresent(wordContainer.word(), (uid, old) -> Math.max(wordContainer.count(), old));
      })), 30, TimeUnit.SECONDS);

      final List<String> source = Stream.generate(() -> text(100))
              .limit(10).collect(toList());
      final Map<String, Integer> expected = source.stream()
              .map(text ->text.split("\\s"))
              .flatMap(Arrays::stream)
              .collect(toMap(Function.identity(), o -> 1, Integer::sum));

      final Consumer<Object> sink = stage.randomFrontConsumer(123);
      source.forEach(sink);
      stage.waitTick(35, TimeUnit.SECONDS);

      Assert.assertEquals(actual, expected);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String text(int size) {
    final String[] words = {"repka", "dedka", "babka", "zhuchka", "vnuchka"};
    return new Random().ints(size, 0, words.length)
            .mapToObj(i -> words[i])
            .collect(joining(" "));
  }

  public static TheGraph wordCountGraph(Collection<Integer> fronts, ActorPath consumer) {
    final FlatMap<String, WordEntry> splitter = new FlatMap<>(new Function<String, Stream<WordEntry>>() {
      @Override
      public Stream<WordEntry> apply(String s) {
        return Arrays.stream(s.split("\\s")).map(WordEntry::new);
      }
    }, HashFunction.OBJECT_HASH);
    final Merge<WordContainer> merge = new Merge<>(Arrays.asList(WORD_HASH, WORD_HASH));
    final Grouping<WordContainer> grouping = new Grouping<>(WORD_HASH, EQUALZ, 2);
    final Filter<List<WordContainer>> filter = new Filter<>(new WrongOrderingFilter(), GROUP_HASH);
    final StatelessMap<List<WordContainer>, WordCounter> counter = new StatelessMap<>(new CountWordEntries(), GROUP_HASH);
    final Broadcast<WordCounter> broadcast = new Broadcast<>(WORD_HASH, 2);

    final PreSinkMetaFilter<WordCounter> metaFilter = new PreSinkMetaFilter<>(WORD_HASH);
    final RemoteActorConsumer<WordCounter> sink = new RemoteActorConsumer<>(consumer);

    final Graph graph =
            splitter.fuse(merge, splitter.outPort(), merge.inPorts().get(0))
            .fuse(grouping, merge.outPort(), grouping.inPort())
            .fuse(filter, grouping.outPort(), filter.inPort())
            .fuse(counter, filter.outPort(), counter.inPort())
            .fuse(broadcast, counter.outPort(), broadcast.inPort())
            .fuse(metaFilter, broadcast.outPorts().get(0), metaFilter.inPort())
            .fuse(sink, metaFilter.outPort(), sink.inPort())
            .wire(broadcast.outPorts().get(1), merge.inPorts().get(1));

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(toMap(Function.identity(), e -> splitter.inPort()));
    return new TheGraph(graph, frontBindings);
  }
}
