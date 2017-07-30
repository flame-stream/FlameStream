package com.spbsu.datastream.core.inverted_index.ranking;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 11.07.2017
 */
public interface RankingStorage {
  void add(String term, int count, Document document);

  int termCountInDoc(String term, Document document);

  int docCountWithTerm(String term);

  int docLength(Document document);

  double avgDocsLength();

  Stream<Document> allDocs();
}
