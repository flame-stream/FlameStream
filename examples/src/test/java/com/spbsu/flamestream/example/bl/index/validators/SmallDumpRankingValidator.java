package com.spbsu.flamestream.example.bl.index.validators;

import com.spbsu.flamestream.example.bl.index.InvertedIndexValidator;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.ranking.Document;
import com.spbsu.flamestream.example.bl.index.ranking.RankingStorage;
import com.spbsu.flamestream.example.bl.index.utils.WikipeadiaInput;
import org.testng.Assert;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 19.12.2017
 */
public class SmallDumpRankingValidator extends InvertedIndexValidator.Stub {
  @Override
  public Stream<WikipediaPage> input() {
    return WikipeadiaInput.dumpStreamFromResources("wikipedia/test_index_ranking_storage_small_dump.xml");
  }

  @Override
  public void assertCorrect(Stream<WordBase> output) {
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

  @Override
  public int expectedOutputSize() {
    return 6245; //experimentally computed
  }
}
