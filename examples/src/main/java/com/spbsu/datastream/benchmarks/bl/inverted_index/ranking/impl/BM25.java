package com.spbsu.datastream.benchmarks.bl.inverted_index.ranking.impl;

import com.google.common.collect.Lists;
import com.spbsu.commons.text.lexical.StemsTokenizer;
import com.spbsu.commons.text.lexical.Tokenizer;
import com.spbsu.commons.text.stem.Stemmer;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ranking.Rank;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ranking.RankingStorage;
import com.spbsu.datastream.benchmarks.bl.inverted_index.ranking.RankingFunction;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;

import java.util.List;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 31.07.2017
 */
public class BM25 implements RankingFunction {
  private final RankingStorage rankingStorage;

  public BM25(RankingStorage rankingStorage) {
    this.rankingStorage = rankingStorage;
  }

  @Override
  public Stream<Rank> rank(CharSequence query) {
    final double k1 = 2.0;
    final double b = 0.75;

    final int docsInCollection = rankingStorage.docsCount();
    final double avgDocsLength = rankingStorage.avgDocsLength();
    final TObjectDoubleMap<CharSequence> idfCache = new TObjectDoubleHashMap<>();

    //noinspection deprecation
    final Tokenizer tokenizer = new StemsTokenizer(Stemmer.getInstance(), query);
    final List<CharSequence> queryTerms = Lists.newArrayList(tokenizer);
    return rankingStorage.allDocs().map(document -> {
      final int docLength = rankingStorage.docLength(document);
      double bm25TotalScore = 0.0;
      for (final CharSequence term : queryTerms) {
        final double idf;
        if (idfCache.containsKey(term)) {
          idf = idfCache.get(term);
        } else {
          final int docsWithTerm = rankingStorage.docCountWithTerm(term);
          idf = Math.log10(((double) docsInCollection - (double) docsWithTerm + 0.5) / ((double) docsWithTerm + 0.5));
          idfCache.put(term, idf);
        }
        if (idf < 0)
          continue; //ignore too frequent words
        final double freq = (double) rankingStorage.termCountInDoc(term, document) / (double) docLength;
        final double bm25TermScore = idf * ((freq * (k1 + 1.0)) / (freq + k1 * (1.0 - b + b * ((double) docLength / avgDocsLength))));
        bm25TotalScore += bm25TermScore;
      }
      return new Rank(document, bm25TotalScore);
    });
  }
}
