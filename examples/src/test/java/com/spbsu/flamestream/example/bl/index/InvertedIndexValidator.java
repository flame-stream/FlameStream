package com.spbsu.flamestream.example.bl.index;

import com.expleague.commons.text.stem.Stemmer;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordBase;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.ranking.Document;
import com.spbsu.flamestream.example.bl.index.ranking.RankingStorage;
import com.spbsu.flamestream.example.bl.index.ranking.impl.InMemRankingStorage;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public interface InvertedIndexValidator {
  Stream<WikipediaPage> input();

  void assertCorrect(Stream<WordBase> output);

  int expectedOutputSize();

  abstract class Stub implements InvertedIndexValidator {
    protected static String stem(String term) {
      //noinspection deprecation
      final Stemmer stemmer = Stemmer.getInstance();
      return stemmer.stem(term).toString();
    }

    protected static RankingStorage rankingStorage(Stream<WordBase> output) {
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
}
