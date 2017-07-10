package com.spbsu.datastream.core.inverted_index;

import akka.actor.ActorPath;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.TestStand;
import com.spbsu.datastream.core.barrier.PreSinkMetaFilter;
import com.spbsu.datastream.core.barrier.RemoteActorConsumer;
import com.spbsu.datastream.core.graph.Graph;
import com.spbsu.datastream.core.graph.InPort;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.*;
import com.spbsu.datastream.core.inverted_index.model.WikipediaPage;
import com.spbsu.datastream.core.inverted_index.model.WordContainer;
import com.spbsu.datastream.core.inverted_index.model.WordPagePositions;
import com.spbsu.datastream.core.inverted_index.ops.*;
import com.spbsu.datastream.core.inverted_index.utils.WikipediaPageIterator;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * User: Artem
 * Date: 10.07.2017
 */
public class InvertedIndexTest {
  private static final HashFunction<WikipediaPage> WIKI_PAGE_HASH = new HashFunction<WikipediaPage>() {
    @Override
    public boolean equal(WikipediaPage o1, WikipediaPage o2) {
      return o1.id() == o2.id();
    }

    @Override
    public int hash(WikipediaPage value) {
      return value.id();
    }
  };

  private static final HashFunction<WordContainer> WORD_HASH = new HashFunction<WordContainer>() {
    @Override
    public boolean equal(WordContainer o1, WordContainer o2) {
      return o1.word().equals(o2.word());
    }

    @Override
    public int hash(WordContainer value) {
      return value.word().hashCode();
    }
  };

  private static final HashFunction<List<WordContainer>> GROUP_HASH = new HashFunction<List<WordContainer>>() {
    @Override
    public boolean equal(List<WordContainer> o1, List<WordContainer> o2) {
      return WORD_HASH.equal(o1.get(0), o2.get(0));
    }

    @Override
    public int hash(List<WordContainer> value) {
      return WORD_HASH.hash(value.get(0));
    }
  };

  @Test(enabled = false)
  public void testIndexWithSmallDump() throws InterruptedException, FileNotFoundException {
    final ClassLoader classLoader = InvertedIndexTest.class.getClassLoader();
    final URL fileUrl = classLoader.getResource("wikipedia/small_dump_example.xml");
    if (fileUrl == null) {
      throw new RuntimeException("Dump URL is null");
    }

    final File dumpFile = new File(fileUrl.getFile());
    final InputStream inputStream = new FileInputStream(dumpFile);
    final Iterator<WikipediaPage> wikipediaPageIterator = new WikipediaPageIterator(inputStream);
    final Iterable<WikipediaPage> iterable = () -> wikipediaPageIterator;
    final Stream<WikipediaPage> source = StreamSupport.stream(iterable.spliterator(), false);
    this.test(source, 20, 1);
  }

  @SuppressWarnings("SameParameterValue")
  private void test(Stream<WikipediaPage> source, int tickLength, int fronts) throws InterruptedException {
    try (TestStand stage = new TestStand(4, fronts)) {
      stage.deploy(invertedIndexTest(stage.fronts(), stage.wrap(System.out::println)), tickLength, TimeUnit.SECONDS);
      final Consumer<Object> sink = stage.randomFrontConsumer(123);
      source.forEach(sink);
      stage.waitTick(tickLength + 5, TimeUnit.SECONDS);
    }
  }

  private static TheGraph invertedIndexTest(Collection<Integer> fronts, ActorPath consumer) {
    final FlatFilter<WikipediaPage, WordPagePositions> wikiPageToPositions = new FlatFilter<>(new WikipediaPageToWordPositions(), WIKI_PAGE_HASH);
    final Merge<WordContainer> merge = new Merge<>(Arrays.asList(WORD_HASH, WORD_HASH));
    final Filter<WordContainer> indexDiffFilter = new Filter<>(new WordIndexDiffFilter(), WORD_HASH);
    final Grouping<WordContainer> grouping = new Grouping<>(WORD_HASH, 2);
    final Filter<List<WordContainer>> wrongOrderingFilter = new Filter<>(new WrongOrderingFilter(), GROUP_HASH);
    final FlatFilter<List<WordContainer>, WordContainer> indexer = new FlatFilter<>(new WordIndexToDiffOutput(), GROUP_HASH);
    final Filter<WordContainer> indexFilter = new Filter<>(new WordIndexFilter(), WORD_HASH);
    final Broadcast<WordContainer> broadcast = new Broadcast<>(WORD_HASH, 2);

    final PreSinkMetaFilter<WordContainer> metaFilter = new PreSinkMetaFilter<>(WORD_HASH);
    final RemoteActorConsumer<WordContainer> sink = new RemoteActorConsumer<>(consumer);

    final Graph graph = wikiPageToPositions.fuse(merge, wikiPageToPositions.outPort(), merge.inPorts().get(0))
            .fuse(grouping, merge.outPort(), grouping.inPort())
            .fuse(wrongOrderingFilter, grouping.outPort(), wrongOrderingFilter.inPort())
            .fuse(indexer, wrongOrderingFilter.outPort(), indexer.inPort())
            .fuse(broadcast, indexer.outPort(), broadcast.inPort())
            .fuse(indexFilter, broadcast.outPorts().get(0), indexFilter.inPort())
            .fuse(metaFilter, indexFilter.outPort(), metaFilter.inPort())
            .fuse(sink, metaFilter.outPort(), sink.inPort())
            .fuse(indexDiffFilter, broadcast.outPorts().get(1), indexDiffFilter.inPort())
            .wire(indexDiffFilter.outPort(), merge.inPorts().get(1));

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> wikiPageToPositions.inPort()));
    return new TheGraph(graph, frontBindings);
  }
}
