package com.spbsu.datastream.benchmarks.bl.inverted_index;

import com.spbsu.commons.text.stem.Stemmer;
import com.spbsu.datastream.benchmarks.bl.DataStreamsSource;
import com.spbsu.datastream.benchmarks.bl.FlinkSource;
import com.spbsu.datastream.benchmarks.bl.TestSource;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WikipediaPage;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WordContainer;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WordIndexAdd;
import com.spbsu.datastream.benchmarks.bl.inverted_index.model.WordIndexRemove;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ranking.Document;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ranking.Rank;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ranking.RankingFunction;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ranking.RankingStorage;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ranking.impl.BM25;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ranking.impl.InMemRankingStorage;
import com.spbsu.datastream.benchmarks.bl.inverted_index.utils.IndexLongUtil;
import com.spbsu.datastream.benchmarks.bl.inverted_index.utils.InputUtils;
import com.spbsu.datastream.core.LocalCluster;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 10.07.2017
 */
@SuppressWarnings("MagicNumber")
public class InvertedIndexTest {
  private static final FlinkSource FLINK_INDEX_SOURCE = new FlinkSource<WikipediaPage>(0) {
    @Override
    public void test(Stream<WikipediaPage> input, Consumer<Object> output) {
      InvertedIndexFlinkRunner.test(input, output, this.bufferTimeout);
    }
  };

  @DataProvider
  public Object[][] testIndexWithSmallDumpSources() {
    return new Object[][]{
            {dataStreamsSource(2, 4, 5)},
            {FLINK_INDEX_SOURCE}
    };
  }

  @Test(dataProvider = "testIndexWithSmallDumpSources")
  public void testIndexWithSmallDump(TestSource testSource) throws Exception {
    final Stream<WikipediaPage> source = InputUtils.dumpStreamFromResources("wikipedia/test_index_small_dump.xml");
    final List<WordContainer> output = new ArrayList<>();

    //noinspection unchecked
    testSource.test(source, o -> output.add((WordContainer) o));//rankingStorage(source, o -> output.add((WordContainer) o), 2, 4, 5);
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

  @DataProvider
  public Object[][] testIndexAndRankingStorageWithSmallDumpSources() {
    return new Object[][]{
            {dataStreamsSource(4, 4, 10)},
            {FLINK_INDEX_SOURCE}
    };
  }

  @Test(dataProvider = "testIndexAndRankingStorageWithSmallDumpSources")
  public void testIndexAndRankingStorageWithSmallDump(TestSource testSource) throws Exception {
    final Stream<WikipediaPage> source = InputUtils.dumpStreamFromResources("wikipedia/test_index_ranking_storage_small_dump.xml");
    final RankingStorage rankingStorage = rankingStorage(source, testSource);

    final Document litvaDoc = new Document(7, 2);
    final Document slonovyeDoc = new Document(10, 1);
    final Document mamontyDoc = new Document(11, 1);
    final Document krasnayaKnigaDoc = new Document(15, 1);
    {
      //Assert.assertEquals(rankingStorage.avgDocsLength(), 2157.5);
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
      Assert.assertEquals(rankingStorage.allDocs().count(), 4);
      Assert.assertTrue(rankingStorage.allDocs().anyMatch(document -> document.equals(litvaDoc)));
      Assert.assertTrue(rankingStorage.allDocs().anyMatch(document -> document.equals(slonovyeDoc)));
      Assert.assertTrue(rankingStorage.allDocs().anyMatch(document -> document.equals(mamontyDoc)));
      Assert.assertTrue(rankingStorage.allDocs().anyMatch(document -> document.equals(krasnayaKnigaDoc)));
    }
  }

  @DataProvider
  public Object[][] testIndexWithRankingSources() {
    return new Object[][]{
            {dataStreamsSource(1, 1, 40)},
            {FLINK_INDEX_SOURCE}
    };
  }

  @Test(dataProvider = "testIndexWithRankingSources")
  public void testIndexWithRanking(TestSource testSource) throws Exception {
    final Stream<WikipediaPage> source = InputUtils.dumpStreamFromResources("wikipedia/national_football_teams_dump.xml");
    final RankingStorage rankingStorage = rankingStorage(source, testSource);
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
  public Object[][] manualTestIndexWithRankingSource() {
    return new Object[][]{
            {dataStreamsSource(1, 1, 10), "Звонимир Бобан"},
            {FLINK_INDEX_SOURCE, "Звонимир Бобан"}
    };
  }

  //Enable rankingStorage, set queries and have fun!
  @Test(enabled = false, dataProvider = "manualTestIndexWithRankingSource")
  public void manualTestIndexWithRanking(TestSource testSource, String query) throws Exception {
    final TIntObjectMap<String> docsTitleResolver = new TIntObjectHashMap<>();
    final Stream<WikipediaPage> source = InputUtils.dumpStreamFromResources("wikipedia/national_football_teams_dump.xml")
            .peek(wikipediaPage -> docsTitleResolver.put(wikipediaPage.id(), wikipediaPage.title()));
    final RankingStorage rankingStorage = rankingStorage(source, testSource);
    final RankingFunction rankingFunction = new BM25(rankingStorage);

    System.out.println("Query: " + query);
    rankingFunction.rank(query).sorted().limit(10).forEach(rank -> System.out.println(docsTitleResolver.get(rank.document().id()) + " (" + rank.document().id() + ") : " + rank.score()));
  }

  @SuppressWarnings("SameParameterValue")
  private static RankingStorage rankingStorage(Stream<WikipediaPage> stream, TestSource testSource) throws Exception {
    final RankingStorage rankingStorage = new InMemRankingStorage();
    //noinspection unchecked
    testSource.test(stream, container -> {
      if (container instanceof WordIndexAdd) {
        final WordIndexAdd indexAdd = (WordIndexAdd) container;
        final int docId = IndexLongUtil.pageId(indexAdd.positions()[0]);
        final int docVersion = IndexLongUtil.version(indexAdd.positions()[0]);
        rankingStorage.add(indexAdd.word(), indexAdd.positions().length, new Document(docId, docVersion));
      }
    });
    return rankingStorage;
  }

  private static String stem(String term) {
    //noinspection deprecation
    final Stemmer stemmer = Stemmer.getInstance();
    return stemmer.stem(term).toString();
  }

  private DataStreamsSource<WikipediaPage> dataStreamsSource(int fronts, int workers, int tickLength) {
    return new DataStreamsSource<WikipediaPage>(fronts, workers, tickLength) {
      @Override
      public void test(Stream<WikipediaPage> input, Consumer<Object> output) {
        try {
          try (final LocalCluster cluster = new LocalCluster(this.workers, this.fronts)) {
            InvertedIndexRunner.test(cluster, input, output, this.tickLength);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
