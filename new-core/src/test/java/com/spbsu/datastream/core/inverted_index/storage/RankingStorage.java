package com.spbsu.datastream.core.inverted_index.storage;

/**
 * User: Artem
 * Date: 11.07.2017
 */
public interface RankingStorage {
  void add(String term, int count, int docId, int docVersion);

  int termCountInDoc(String term, int docId);

  int docCountWithTerm(String term);

  int docLength(int docId);

  double avgDocsLength();
}
