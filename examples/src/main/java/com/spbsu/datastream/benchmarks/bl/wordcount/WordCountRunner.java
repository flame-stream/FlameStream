package com.spbsu.datastream.benchmarks.bl.wordcount;

import akka.actor.ActorPath;
import com.spbsu.datastream.benchmarks.ClusterRunner;
import com.spbsu.datastream.benchmarks.bl.wordcount.model.WordContainer;
import com.spbsu.datastream.benchmarks.bl.wordcount.model.WordCounter;
import com.spbsu.datastream.benchmarks.bl.wordcount.model.WordEntry;
import com.spbsu.datastream.benchmarks.bl.wordcount.ops.CountWordEntries;
import com.spbsu.datastream.benchmarks.bl.wordcount.ops.WordContainerOrderingFilter;
import com.spbsu.datastream.benchmarks.measure.LatencyMeasurer;
import com.spbsu.datastream.core.Cluster;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.TestStand;
import com.spbsu.datastream.core.barrier.BarrierSink;
import com.spbsu.datastream.core.barrier.PreBarrierMetaFilter;
import com.spbsu.datastream.core.barrier.RemoteActorSink;
import com.spbsu.datastream.core.graph.ChaincallGraph;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.Broadcast;
import com.spbsu.datastream.core.graph.ops.Filter;
import com.spbsu.datastream.core.graph.ops.FlatMap;
import com.spbsu.datastream.core.graph.ops.Grouping;
import com.spbsu.datastream.core.graph.ops.Merge;
import com.spbsu.datastream.core.graph.ops.StatelessMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

@SuppressWarnings("Convert2Lambda")
public final class WordCountRunner implements ClusterRunner {
  private final Logger LOG = LoggerFactory.getLogger(WordCountRunner.class);

  @Override
  public void run(Cluster cluster) throws InterruptedException {
    final LatencyMeasurer<WordCounter> latencyMeasurer = new LatencyMeasurer<>(1000 * 10, 1000 * 10);

    final TObjectIntMap<String> expected = new TObjectIntHashMap<>();
    final Stream<String> input = input()
            .peek(
                    text -> {
                      final Pattern pattern = Pattern.compile("\\s");
                      Arrays.stream(pattern.split(text))
                              .collect(toMap(Function.identity(), o -> 1, Integer::sum))
                              .forEach((k, v) -> {
                                expected.adjustOrPutValue(k, v, v);
                                latencyMeasurer.start(new WordCounter(k, expected.get(k)));
                              });
                    }
            );

    test(cluster, input, o -> latencyMeasurer.finish((WordCounter) o), 60);
    final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
    LOG.info("Result: {}", stat);
  }

  static Stream<String> input() {
    return Stream.generate(() -> new Random().ints(1000, 0, 1000)
            .mapToObj(num -> "word" + num).collect(joining(" ")))
            .limit(500);
  }

  static void test(Cluster cluster,
                   Stream<String> source,
                   Consumer<Object> outputConsumer,
                   int tickLengthSeconds) throws InterruptedException {
    try (final TestStand stage = new TestStand(cluster)) {
      stage.deploy(chainGraph(stage.frontIds(), stage.wrap(outputConsumer)), tickLengthSeconds, TimeUnit.SECONDS);

      final Consumer<Object> sink = stage.randomFrontConsumer(1);
      //noinspection Duplicates
      source.forEach(s -> {
        sink.accept(s);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException();
        }
      });
      stage.waitTick(20, TimeUnit.SECONDS);
    }
  }

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

  private static TheGraph chainGraph(Collection<Integer> fronts, ActorPath consumerPath) {
    final Merge merge = new Merge(Arrays.asList(WORD_HASH, WORD_HASH));
    final Grouping<WordContainer> grouping = new Grouping<>(WORD_HASH, EQUALZ, 2);
    final Filter<List<WordContainer>> filter = new Filter<>(new WordContainerOrderingFilter(), GROUP_HASH);
    final StatelessMap<List<WordContainer>, WordCounter> counter = new StatelessMap<>(new CountWordEntries(), GROUP_HASH);
    final Broadcast<WordCounter> broadcast = new Broadcast<>(WORD_HASH, 2);
    final PreBarrierMetaFilter<WordCounter> metaFilter = new PreBarrierMetaFilter<>(WORD_HASH);

    final ChaincallGraph logicChain = new ChaincallGraph(
            merge.fuse(grouping, merge.outPort(), grouping.inPort())
                    .fuse(filter, grouping.outPort(), filter.inPort())
                    .fuse(counter, filter.outPort(), counter.inPort())
                    .fuse(broadcast, counter.outPort(), broadcast.inPort())
                    .fuse(metaFilter, broadcast.outPorts().get(0), metaFilter.inPort())
                    .wire(broadcast.outPorts().get(1), merge.inPorts().get(1))
                    .flattened()
    );

    final FlatMap<String, WordEntry> splitter = new FlatMap<>(new Function<String, Stream<WordEntry>>() {
      @Override
      public Stream<WordEntry> apply(String s) {
        return Arrays.stream(s.split("\\s")).map(WordEntry::new);
      }
    }, HashFunction.OBJECT_HASH);

    final RemoteActorSink sink = new RemoteActorSink(consumerPath);
    final BarrierSink barrierSink = new BarrierSink(sink);

    final Graph graph = splitter
            .fuse(logicChain, splitter.outPort(), merge.inPorts().get(0))
            .fuse(barrierSink, metaFilter.outPort(), barrierSink.inPort());

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(toMap(Function.identity(), e -> splitter.inPort()));
    return new TheGraph(graph, frontBindings);
  }
}
