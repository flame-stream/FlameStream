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
import com.spbsu.datastream.core.inverted_index.datastreams.model.*;
import com.spbsu.datastream.core.inverted_index.datastreams.ops.*;
import com.spbsu.datastream.core.inverted_index.datastreams.utils.IndexLongUtil;
import com.spbsu.datastream.core.inverted_index.datastreams.utils.WikipediaPageIterator;
import com.spbsu.datastream.core.inverted_index.storage.InMemRankingStorage;
import com.spbsu.datastream.core.inverted_index.storage.RankingStorage;
import org.testng.Assert;
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

  @Test
  public void testIndexWithSmallDump() throws InterruptedException, FileNotFoundException {
    final File dumpFile = fileFromResources("wikipedia/test_index_small_dump.xml");
    final List<WordContainer> output = new ArrayList<>();

    test(dumpFile, o -> output.add((WordContainer) o), 2, 5);

    Assert.assertEquals(output.size(), 4411);
    { //assertions for word "isbn"
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> "isbn".equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                      {
                              IndexLongUtil.createPagePosition(7, 2534, 1),
                              IndexLongUtil.createPagePosition(7, 2561, 1)
                      })));
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> "isbn".equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexRemove)
              .allMatch(indexRemove -> ((WordIndexRemove) indexRemove).start() == IndexLongUtil.createPagePosition(7, 2534, 1) && ((WordIndexRemove) indexRemove).range() == 2));
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> "isbn".equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                      {
                              IndexLongUtil.createPagePosition(7, 2561, 2)
                      })));
    }
    { //assertions for word "вставка"
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> "вставка".equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .allMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                      {
                              IndexLongUtil.createPagePosition(7, 2515, 2)
                      })));
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> "вставка".equals(wordContainer.word()))
              .noneMatch(wordContainer -> wordContainer instanceof WordIndexRemove));
    }
    { //assertions for word "эйдинтас"
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> "эйдинтас".equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                      {
                              IndexLongUtil.createPagePosition(7, 2516, 1)
                      })));
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> "эйдинтас".equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexRemove)
              .allMatch(indexRemove -> ((WordIndexRemove) indexRemove).start() == IndexLongUtil.createPagePosition(7, 2516, 1) && ((WordIndexRemove) indexRemove).range() == 1));
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> "эйдинтас".equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                      {
                              IndexLongUtil.createPagePosition(7, 2517, 2)
                      })));
    }
  }

  @Test
  public void testIndexAndRankingStorageWithSmallDump() throws InterruptedException, FileNotFoundException {
    final File dumpFile = fileFromResources("wikipedia/test_index_ranking_storage_small_dump.xml");
    final RankingStorage rankingStorage = new InMemRankingStorage();

    test(dumpFile, container -> {
      if (container instanceof WordIndexAdd) {
        final WordIndexAdd indexAdd = (WordIndexAdd) container;
        final int docId = IndexLongUtil.pageId(indexAdd.positions()[0]);
        final int docVersion = IndexLongUtil.version(indexAdd.positions()[0]);
        rankingStorage.add(indexAdd.word(), indexAdd.positions().length, docId, docVersion);
      }
    }, 4, 10);

    {
      Assert.assertEquals(rankingStorage.avgDocsLength(), 2157.5);
      Assert.assertEquals(rankingStorage.docLength(7), 2563);
      Assert.assertEquals(rankingStorage.docLength(10), 2174);
      Assert.assertEquals(rankingStorage.docLength(11), 2937);
      Assert.assertEquals(rankingStorage.docLength(15), 956);
    }
    {
      Assert.assertEquals(rankingStorage.docCountWithTerm("слон"), 2);
      Assert.assertEquals(rankingStorage.termCountInDoc("слон", 10), 29);
      Assert.assertEquals(rankingStorage.termCountInDoc("слон", 11), 1);

      Assert.assertEquals(rankingStorage.docCountWithTerm("россия"), 2);
      Assert.assertEquals(rankingStorage.termCountInDoc("россия", 10), 3);
      Assert.assertEquals(rankingStorage.termCountInDoc("россия", 15), 1);

      Assert.assertEquals(rankingStorage.docCountWithTerm("литва"), 1);
      Assert.assertEquals(rankingStorage.termCountInDoc("литва", 7), 13);
      Assert.assertEquals(rankingStorage.termCountInDoc("литва", 10), 0);
      Assert.assertEquals(rankingStorage.termCountInDoc("литва", 15), 0);
      Assert.assertEquals(rankingStorage.termCountInDoc("литва", 222), 0);
    }
  }

  private static void test(File dumpFile, Consumer<Object> outputConsumer, int fronts, int tickLength) throws InterruptedException, FileNotFoundException {
    final InputStream inputStream = new FileInputStream(dumpFile);
    final Iterator<WikipediaPage> wikipediaPageIterator = new WikipediaPageIterator(inputStream);
    final Iterable<WikipediaPage> iterable = () -> wikipediaPageIterator;
    final Stream<WikipediaPage> source = StreamSupport.stream(iterable.spliterator(), false);

    try (TestStand stage = new TestStand(4, fronts)) {
      stage.deploy(invertedIndexTest(stage.fronts(), stage.wrap(outputConsumer)), tickLength, TimeUnit.SECONDS);
      final Consumer<Object> sink = stage.randomFrontConsumer(122);
      source.forEach(sink);
      stage.waitTick(tickLength + 5, TimeUnit.SECONDS);
    }
  }

  private static TheGraph invertedIndexTest(Collection<Integer> fronts, ActorPath consumer) {
    final FlatMap<WikipediaPage, WordPagePositions> wikiPageToPositions = new FlatMap<>(new WikipediaPageToWordPositions(), WIKI_PAGE_HASH);
    final Merge<WordContainer> merge = new Merge<>(Arrays.asList(WORD_HASH, WORD_HASH));
    final Filter<WordContainer> indexDiffFilter = new Filter<>(new WordIndexDiffFilter(), WORD_HASH);
    final Grouping<WordContainer> grouping = new Grouping<>(WORD_HASH, 2);
    final Filter<List<WordContainer>> wrongOrderingFilter = new Filter<>(new WrongOrderingFilter(), GROUP_HASH);
    final FlatMap<List<WordContainer>, WordContainer> indexer = new FlatMap<>(new WordIndexToDiffOutput(), GROUP_HASH);
    final Filter<WordContainer> indexFilter = new Filter<>(new WordIndexFilter(), WORD_HASH);
    final Broadcast<WordContainer> broadcast = new Broadcast<>(WORD_HASH, 2);

    final PreSinkMetaFilter<WordContainer> metaFilter = new PreSinkMetaFilter<>(WORD_HASH);
    final RemoteActorConsumer<WordContainer> sink = new RemoteActorConsumer<>(consumer);

    final Graph graph = wikiPageToPositions.fuse(merge, wikiPageToPositions.outPort(), merge.inPorts().get(0))
            .fuse(grouping, merge.outPort(), grouping.inPort())
            .fuse(wrongOrderingFilter, grouping.outPort(), wrongOrderingFilter.inPort())
            .fuse(indexer, wrongOrderingFilter.outPort(), indexer.inPort())
            .fuse(broadcast, indexer.outPort(), broadcast.inPort())
            .fuse(indexFilter, broadcast.outPorts().get(1), indexFilter.inPort())
            .fuse(metaFilter, indexFilter.outPort(), metaFilter.inPort())
            .fuse(sink, metaFilter.outPort(), sink.inPort())
            .fuse(indexDiffFilter, broadcast.outPorts().get(0), indexDiffFilter.inPort())
            .wire(indexDiffFilter.outPort(), merge.inPorts().get(1));

    final Map<Integer, InPort> frontBindings = fronts.stream()
            .collect(Collectors.toMap(Function.identity(), e -> wikiPageToPositions.inPort()));
    return new TheGraph(graph, frontBindings);
  }

  private static File fileFromResources(String fileName) {
    final ClassLoader classLoader = InvertedIndexTest.class.getClassLoader();
    final URL fileUrl = classLoader.getResource(fileName);
    if (fileUrl == null) {
      throw new RuntimeException("Dump URL is null");
    }
    return new File(fileUrl.getFile());
  }
}
