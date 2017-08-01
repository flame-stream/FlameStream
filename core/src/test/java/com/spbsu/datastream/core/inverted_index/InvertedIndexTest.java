package com.spbsu.datastream.core.inverted_index;

import akka.actor.ActorPath;
import com.spbsu.commons.text.stem.Stemmer;
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
import com.spbsu.datastream.core.inverted_index.ranking.Document;
import com.spbsu.datastream.core.inverted_index.ranking.Rank;
import com.spbsu.datastream.core.inverted_index.ranking.RankingFunction;
import com.spbsu.datastream.core.inverted_index.ranking.RankingStorage;
import com.spbsu.datastream.core.inverted_index.ranking.impl.BM25;
import com.spbsu.datastream.core.inverted_index.ranking.impl.InMemRankingStorage;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
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
    final Stream<WikipediaPage> source = dumpFromResources("wikipedia/test_index_small_dump.xml");
    final List<WordContainer> output = new ArrayList<>();

    test(source, o -> output.add((WordContainer) o), 2, 4, 5);
    Assert.assertEquals(output.size(), 3481);
    { //assertions for word "isbn"
      final String isbn = stem("isbn");
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> isbn.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                      {
                              IndexLongUtil.createPagePosition(7, 2534, 1),
                              IndexLongUtil.createPagePosition(7, 2561, 1)
                      })));
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> isbn.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexRemove)
              .allMatch(indexRemove ->
                      ((WordIndexRemove) indexRemove).start() == IndexLongUtil.createPagePosition(7, 2534, 1) && ((WordIndexRemove) indexRemove).range() == 2));
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> isbn.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                      {
                              IndexLongUtil.createPagePosition(7, 2561, 2)
                      })));
    }
    { //assertions for word "вставка"
      final String vstavka = stem("вставка");
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> vstavka.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .allMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                      {
                              IndexLongUtil.createPagePosition(7, 2515, 2)
                      })));
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> vstavka.equals(wordContainer.word()))
              .noneMatch(wordContainer -> wordContainer instanceof WordIndexRemove));
    }
    { //assertions for word "эйдинтас"
      final String eidintas = stem("эйдинтас");
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> eidintas.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                      {
                              IndexLongUtil.createPagePosition(7, 2516, 1)
                      })));
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> eidintas.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexRemove)
              .allMatch(indexRemove ->
                      ((WordIndexRemove) indexRemove).start() == IndexLongUtil.createPagePosition(7, 2516, 1) && ((WordIndexRemove) indexRemove).range() == 1));
      Assert.assertTrue(output.stream()
              .filter(wordContainer -> eidintas.equals(wordContainer.word()))
              .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
              .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]
                      {
                              IndexLongUtil.createPagePosition(7, 2517, 2)
                      })));
    }
  }

  @Test
  public void testIndexAndRankingStorageWithSmallDump() throws InterruptedException, FileNotFoundException {
    final Stream<WikipediaPage> source = dumpFromResources("wikipedia/test_index_ranking_storage_small_dump.xml");
    final RankingStorage rankingStorage = test(source, 4, 4, 10);

    final Document litvaDoc = new Document(7, 2);
    final Document slonovyeDoc = new Document(10, 1);
    final Document mamontyDoc = new Document(11, 1);
    final Document krasnayaKnigaDoc = new Document(15, 1);
    {
      Assert.assertEquals(rankingStorage.avgDocsLength(), 2157.5);
      Assert.assertEquals(rankingStorage.docLength(litvaDoc), 2563);
      Assert.assertEquals(rankingStorage.docLength(slonovyeDoc), 2174);
      Assert.assertEquals(rankingStorage.docLength(mamontyDoc), 2937);
      Assert.assertEquals(rankingStorage.docLength(krasnayaKnigaDoc), 956);
    }
    {
      final String slon = stem("слон");
      Assert.assertEquals(rankingStorage.docCountWithTerm(slon), 2);
      Assert.assertEquals(rankingStorage.termCountInDoc(slon, slonovyeDoc), 128);
      Assert.assertEquals(rankingStorage.termCountInDoc(slon, mamontyDoc), 12);

      final String rossiya = stem("россия");
      Assert.assertEquals(rankingStorage.docCountWithTerm(rossiya), 3);
      Assert.assertEquals(rankingStorage.termCountInDoc(rossiya, slonovyeDoc), 4);
      Assert.assertEquals(rankingStorage.termCountInDoc(rossiya, krasnayaKnigaDoc), 1);

      final String litva = stem("литва");
      Assert.assertEquals(rankingStorage.docCountWithTerm(litva), 1);
      Assert.assertEquals(rankingStorage.termCountInDoc(litva, litvaDoc), 61);
      Assert.assertEquals(rankingStorage.termCountInDoc(litva, slonovyeDoc), 0);
      Assert.assertEquals(rankingStorage.termCountInDoc(litva, krasnayaKnigaDoc), 0);
    }
    {
      Assert.assertEquals(4, rankingStorage.allDocs().count());
      Assert.assertTrue(rankingStorage.allDocs().anyMatch(document -> document.equals(litvaDoc)));
      Assert.assertTrue(rankingStorage.allDocs().anyMatch(document -> document.equals(slonovyeDoc)));
      Assert.assertTrue(rankingStorage.allDocs().anyMatch(document -> document.equals(mamontyDoc)));
      Assert.assertTrue(rankingStorage.allDocs().anyMatch(document -> document.equals(krasnayaKnigaDoc)));
    }
  }

  @Test
  public void testIndexWithRanking() throws InterruptedException, FileNotFoundException {
    final Stream<WikipediaPage> source = dumpFromResources("wikipedia/national_football_teams_dump.xml");
    final RankingStorage rankingStorage = test(source, 1, 1, 10);
    final RankingFunction rankingFunction = new BM25(rankingStorage);
    {
      final Stream<Rank> result = rankingFunction.rank("Бразилия Пеле");
      final Rank[] topResults = result.sorted().limit(5).toArray(Rank[]::new);
      Assert.assertEquals(topResults[0], new Rank(new Document(51626, 1), 0.01503891930975921));
      Assert.assertEquals(topResults[1], new Rank(new Document(1027839, 1), 0.013517410031763473));
      Assert.assertEquals(topResults[2], new Rank(new Document(2446853, 1), 0.010903350643125045));
      Assert.assertEquals(topResults[3], new Rank(new Document(227209, 1), 0.008340914850280897));
      Assert.assertEquals(topResults[4], new Rank(new Document(229964, 1), 0.00632081101215173));
    }
    {
      final Stream<Rank> result = rankingFunction.rank("Аргентина Марадона");
      final Rank[] topResults = result.sorted().limit(5).toArray(Rank[]::new);
      Assert.assertEquals(topResults[0], new Rank(new Document(227209, 1), 0.03466819792138674));
      Assert.assertEquals(topResults[1], new Rank(new Document(688695, 1), 0.034573538000985574));
      Assert.assertEquals(topResults[2], new Rank(new Document(879050, 1), 0.030395004860259645));
      Assert.assertEquals(topResults[3], new Rank(new Document(2446853, 1), 0.026082172662643795));
      Assert.assertEquals(topResults[4], new Rank(new Document(1020395, 1), 0.0133369643808426));
    }
  }

  @DataProvider
  public Object[][] queries() {
    return new Object[][]{
            {"Звонимир Бобан"}
    };
  }

  //Enable test, set queries and have fun!
  @Test(enabled = false, dataProvider = "queries")
  public void manualTestIndexWithRanking(String query) throws FileNotFoundException, InterruptedException {
    final TIntObjectMap<String> docsTitleResolver = new TIntObjectHashMap<>();
    final Stream<WikipediaPage> source = dumpFromResources("wikipedia/national_football_teams_dump.xml")
            .peek(wikipediaPage -> docsTitleResolver.put(wikipediaPage.id(), wikipediaPage.title()));
    final RankingStorage rankingStorage = test(source, 1, 1, 10);
    final RankingFunction rankingFunction = new BM25(rankingStorage);

    System.out.println("Query: " + query);
    rankingFunction.rank(query).sorted().limit(10).forEach(rank -> System.out.println(docsTitleResolver.get(rank.document().id()) + " (" + rank.document().id() + ") : " + rank.score()));
  }

  @SuppressWarnings("SameParameterValue")
  private static RankingStorage test(Stream<WikipediaPage> source, int fronts, int workers, int tickLength) throws FileNotFoundException, InterruptedException {
    final RankingStorage rankingStorage = new InMemRankingStorage();
    test(source, container -> {
      if (container instanceof WordIndexAdd) {
        final WordIndexAdd indexAdd = (WordIndexAdd) container;
        final int docId = IndexLongUtil.pageId(indexAdd.positions()[0]);
        final int docVersion = IndexLongUtil.version(indexAdd.positions()[0]);
        rankingStorage.add(indexAdd.word(), indexAdd.positions().length, new Document(docId, docVersion));
      }
    }, fronts, workers, tickLength);
    return rankingStorage;
  }

  private static void test(Stream<WikipediaPage> source, Consumer<Object> outputConsumer, int fronts, int workers, int tickLength) throws InterruptedException, FileNotFoundException {
    try (TestStand stage = new TestStand(workers, fronts)) {
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

  private static Stream<WikipediaPage> dumpFromResources(String dumpPath) throws FileNotFoundException {
    final ClassLoader classLoader = InvertedIndexTest.class.getClassLoader();
    final URL fileUrl = classLoader.getResource(dumpPath);
    if (fileUrl == null) {
      throw new RuntimeException("Dump URL is null");
    }

    final File dumpFile = new File(fileUrl.getFile());
    final InputStream inputStream = new FileInputStream(dumpFile);
    final Iterator<WikipediaPage> wikipediaPageIterator = new WikipediaPageIterator(inputStream);
    final Iterable<WikipediaPage> iterable = () -> wikipediaPageIterator;
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  private static String stem(String term) {
    //noinspection deprecation
    final Stemmer stemmer = Stemmer.getInstance();
    return stemmer.stem(term).toString();
  }
}
