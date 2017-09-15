package com.spbsu.datastream.benchmarks.bl.inverted_index;

import akka.actor.ActorPath;
import com.spbsu.datastream.benchmarks.ClusterRunner;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WikipediaPage;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WordContainer;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WordIndexAdd;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WordPagePositions;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ops.WikipediaPageToWordPositions;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ops.WordIndexDiffFilter;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ops.WordIndexFilter;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ops.WordIndexToDiffOutput;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ops.WrongOrderingFilter;
import com.spbsu.datastream.benchmarks.bl.inverted_index.utils.IndexLongUtil;
import com.spbsu.datastream.benchmarks.bl.inverted_index.utils.InputUtils;
import com.spbsu.datastream.benchmarks.measure.LatencyMeasurer;
import com.spbsu.datastream.core.Cluster;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.TestStand;
import com.spbsu.datastream.core.barrier.PreSinkMetaFilter;
import com.spbsu.datastream.core.barrier.RemoteActorConsumer;
import com.spbsu.datastream.core.graph.AtomicGraph;
import com.spbsu.datastream.core.graph.ChaincallGraph;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.Broadcast;
import com.spbsu.datastream.core.graph.ops.Filter;
import com.spbsu.datastream.core.graph.ops.FlatMap;
import com.spbsu.datastream.core.graph.ops.Grouping;
import com.spbsu.datastream.core.graph.ops.Merge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 18.08.2017
 */
@SuppressWarnings("Convert2Lambda")
public class InvertedIndexRunner implements ClusterRunner {
  private final Logger LOG = LoggerFactory.getLogger(InvertedIndexRunner.class);

  @Override
  public void run(Cluster cluster) throws InterruptedException {
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(100, 0);

    try {
      final Stream<WikipediaPage> source = InputUtils.dumpStreamFromResources("wikipedia/national_football_teams_dump.xml")
              .peek(wikipediaPage -> latencyMeasurer.start(wikipediaPage.id()));
      test(cluster, source, container -> {
        if (container instanceof WordIndexAdd) {
          final WordIndexAdd indexAdd = (WordIndexAdd) container;
          final int docId = IndexLongUtil.pageId(indexAdd.positions()[0]);
          latencyMeasurer.finish(docId);
        }
      }, 40);

      final LongSummaryStatistics stat = Arrays
              .stream(latencyMeasurer.latencies())
              .summaryStatistics();
      LOG.info(Arrays.toString(latencyMeasurer.latencies()));
      LOG.info("Result: {}", stat);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  static void test(Cluster cluster,
                   Stream<WikipediaPage> source,
                   Consumer<Object> outputConsumer,
                   int tickLength) throws InterruptedException {
    try (final TestStand stage = new TestStand(cluster)) {
      stage.deploy(chaincallGraph(stage.frontIds(), stage.wrap(outputConsumer)), tickLength, TimeUnit.SECONDS);

      final Consumer<Object> sink = stage.randomFrontConsumer(122);
      //noinspection Duplicates
      source.forEach(wikipediaPage -> {
        sink.accept(wikipediaPage);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException();
        }
      });

      stage.waitTick(25, TimeUnit.SECONDS);
    }
  }

  private static final HashFunction<WikipediaPage> WIKI_PAGE_HASH = new HashFunction<WikipediaPage>() {
    @Override
    public int hash(WikipediaPage value) {
      return value.id();
    }
  };

  private static final HashFunction<WordContainer> WORD_HASH = new HashFunction<WordContainer>() {
    @Override
    public int hash(WordContainer value) {
      return value.word().hashCode();
    }
  };

  private static final BiPredicate<WordContainer, WordContainer> WORD_EQUALZ = new BiPredicate<WordContainer, WordContainer>() {
    @Override
    public boolean test(WordContainer wordContainer, WordContainer wordContainer2) {
      return wordContainer.word().equals(wordContainer.word());
    }
  };

  private static final HashFunction<List<WordContainer>> GROUP_HASH = new HashFunction<List<WordContainer>>() {
    @Override
    public int hash(List<WordContainer> value) {
      return WORD_HASH.hash(value.get(0));
    }
  };

  private static TheGraph chaincallGraph(Collection<Integer> fronts, ActorPath consumer) {
    final Merge<WordContainer> merge = new Merge<>(Arrays.asList(WORD_HASH, WORD_HASH));
    final Filter<WordContainer> indexDiffFilter = new Filter<>(new WordIndexDiffFilter(), WORD_HASH);
    final Grouping<WordContainer> grouping = new Grouping<>(WORD_HASH, WORD_EQUALZ, 2);
    final Filter<List<WordContainer>> wrongOrderingFilter = new Filter<>(new WrongOrderingFilter(), GROUP_HASH);
    final FlatMap<List<WordContainer>, WordContainer> indexer = new FlatMap<>(new WordIndexToDiffOutput(), GROUP_HASH);
    final Filter<WordContainer> indexFilter = new Filter<>(new WordIndexFilter(), WORD_HASH);
    final Broadcast<WordContainer> broadcast = new Broadcast<>(WORD_HASH, 2);
    final PreSinkMetaFilter<WordContainer> metaFilter = new PreSinkMetaFilter<>(WORD_HASH);

    final AtomicGraph chain = new ChaincallGraph(
            merge.fuse(grouping, merge.outPort(), grouping.inPort())
                    .fuse(wrongOrderingFilter, grouping.outPort(), wrongOrderingFilter.inPort())
                    .fuse(indexer, wrongOrderingFilter.outPort(), indexer.inPort())
                    .fuse(broadcast, indexer.outPort(), broadcast.inPort())
                    .fuse(indexFilter, broadcast.outPorts().get(1), indexFilter.inPort())
                    .fuse(metaFilter, indexFilter.outPort(), metaFilter.inPort())
                    .fuse(indexDiffFilter, broadcast.outPorts().get(0), indexDiffFilter.inPort())
                    .wire(indexDiffFilter.outPort(), merge.inPorts().get(1))
                    .flattened()
    );

    final FlatMap<WikipediaPage, WordPagePositions> wikiPageToPositions = new FlatMap<>(new WikipediaPageToWordPositions(), WIKI_PAGE_HASH);
    final RemoteActorConsumer<WordContainer> sink = new RemoteActorConsumer<>(consumer);

    final Graph graph = wikiPageToPositions.fuse(chain, wikiPageToPositions.outPort(), merge.inPorts().get(0))
            .fuse(sink, metaFilter.outPort(), sink.inPort());
    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> wikiPageToPositions.inPort()));
    return new TheGraph(graph, frontBindings);
  }
}
