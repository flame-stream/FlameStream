package com.spbsu.flamestream.example.bl.index.ranking;

import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 11.07.2017
 */
public interface RankingStorage {
  void add(CharSequence term, int count, Document document);

  int termCountInDoc(CharSequence term, Document document);

  int docCountWithTerm(CharSequence term);

  int docLength(Document document);

  double avgDocsLength();

  Stream<Document> allDocs();

  int docsCount();
}
