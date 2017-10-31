package com.spbsu.flamestream.example.index;

import com.expleague.commons.text.stem.Stemmer;
import com.spbsu.flamestream.example.ExampleChecker;
import com.spbsu.flamestream.example.index.model.WikipediaPage;
import com.spbsu.flamestream.example.index.model.WordBase;
import com.spbsu.flamestream.example.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.index.ranking.Document;
import com.spbsu.flamestream.example.index.ranking.Rank;
import com.spbsu.flamestream.example.index.ranking.RankingFunction;
import com.spbsu.flamestream.example.index.ranking.RankingStorage;
import com.spbsu.flamestream.example.index.ranking.impl.BM25;
import com.spbsu.flamestream.example.index.ranking.impl.InMemRankingStorage;
import com.spbsu.flamestream.example.index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.index.utils.WikipeadiaInput;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public enum InvertedIndexCheckers implements ExampleChecker<WikipediaPage> {
  CHECK_INDEX_WITH_SMALL_DUMP {
    @Override
    public Stream<WikipediaPage> input() {
      return WikipeadiaInput.dumpStreamFromResources("wikipedia/test_index_small_dump.xml");
    }

    @Override
    public void assertCorrect(Stream<Object> output) {
      final List<WordBase> outputList = new ArrayList<>();
      output.forEach(o -> outputList.add((WordBase) o));
      Assert.assertEquals(outputList.size(), 3481);
      { //assertions for word "isbn"
        final String isbn = stem("isbn");
        Assert.assertTrue(outputList.stream()
                .filter(wordContainer -> isbn.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
                .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]{
                        IndexItemInLong.createPagePosition(7, 2534, 1), IndexItemInLong.createPagePosition(7, 2561, 1)
                })));
        Assert.assertTrue(outputList.stream()
                .filter(wordContainer -> isbn.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexRemove)
                .allMatch(indexRemove ->
                        ((WordIndexRemove) indexRemove).start() == IndexItemInLong.createPagePosition(7, 2534, 1)
                                && ((WordIndexRemove) indexRemove).range() == 2));
        Assert.assertTrue(outputList.stream()
                .filter(wordContainer -> isbn.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
                .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]{
                        IndexItemInLong.createPagePosition(7, 2561, 2)
                })));
      }
      { //assertions for word "вставка"
        final String vstavka = stem("вставка");
        Assert.assertTrue(outputList.stream()
                .filter(wordContainer -> vstavka.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
                .allMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]{
                        IndexItemInLong.createPagePosition(7, 2515, 2)
                })));
        Assert.assertTrue(outputList.stream()
                .filter(wordContainer -> vstavka.equals(wordContainer.word()))
                .noneMatch(wordContainer -> wordContainer instanceof WordIndexRemove));
      }
      { //assertions for word "эйдинтас"
        final String eidintas = stem("эйдинтас");
        Assert.assertTrue(outputList.stream()
                .filter(wordContainer -> eidintas.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
                .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]{
                        IndexItemInLong.createPagePosition(7, 2516, 1)
                })));
        Assert.assertTrue(outputList.stream()
                .filter(wordContainer -> eidintas.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexRemove)
                .allMatch(indexRemove ->
                        ((WordIndexRemove) indexRemove).start() == IndexItemInLong.createPagePosition(7, 2516, 1)
                                && ((WordIndexRemove) indexRemove).range() == 1));
        Assert.assertTrue(outputList.stream()
                .filter(wordContainer -> eidintas.equals(wordContainer.word()))
                .filter(wordContainer -> wordContainer instanceof WordIndexAdd)
                .anyMatch(indexAdd -> Arrays.equals(((WordIndexAdd) indexAdd).positions(), new long[]{
                        IndexItemInLong.createPagePosition(7, 2517, 2)
                })));
      }
    }
  },
  CHECK_INDEX_AND_RANKING_STORAGE_WITH_SMALL_DUMP {
    @Override
    public Stream<WikipediaPage> input() {
      return WikipeadiaInput.dumpStreamFromResources("wikipedia/test_index_ranking_storage_small_dump.xml");
    }

    @Override
    public void assertCorrect(Stream<Object> output) {
      final RankingStorage rankingStorage = rankingStorage(output);

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
  },
  CHECK_INDEX_WITH_RANKING {
    @Override
    public Stream<WikipediaPage> input() {
      return WikipeadiaInput.dumpStreamFromResources("wikipedia/national_football_teams_dump.xml");
    }

    @Override
    public void assertCorrect(Stream<Object> output) {
      final RankingStorage rankingStorage = rankingStorage(output);
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
  };

  private static String stem(String term) {
    //noinspection deprecation
    final Stemmer stemmer = Stemmer.getInstance();
    return stemmer.stem(term).toString();
  }

  private static RankingStorage rankingStorage(Stream<Object> output) {
    final RankingStorage rankingStorage = new InMemRankingStorage();
    output.forEach(container -> {
      if (container instanceof WordIndexAdd) {
        final WordIndexAdd indexAdd = (WordIndexAdd) container;
        final int docId = IndexItemInLong.pageId(indexAdd.positions()[0]);
        final int docVersion = IndexItemInLong.version(indexAdd.positions()[0]);
        rankingStorage.add(indexAdd.word(), indexAdd.positions().length, new Document(docId, docVersion));
      }
    });
    return rankingStorage;
  }
}
