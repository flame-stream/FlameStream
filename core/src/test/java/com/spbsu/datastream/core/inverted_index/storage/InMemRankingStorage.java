package com.spbsu.datastream.core.inverted_index.storage;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Artem
 * Date: 11.07.2017
 */
public class InMemRankingStorage implements RankingStorage {
  private final TIntObjectMap<Doc> docs = new TIntObjectHashMap<>();
  private final Map<String, TIntIntMap> termCountInDoc = new HashMap<>();
  private double avgDocsLength = 0.0;

  @Override
  public void add(String term, int count, int docId, int docVersion) {
    final int prevDocsSize = docs.size();
    final Doc doc = docs.get(docId);
    if (doc != null && docVersion > doc.version()) { //uncommon scenario
      termCountInDoc.forEach((t, map) -> {
        if (t.equals(term))
          map.put(docId, count);
        else
          map.remove(docId);
      });
      avgDocsLength = (avgDocsLength * prevDocsSize - doc.length() + count) / (double) prevDocsSize;
      doc.length(count);
      doc.version(docVersion);
    } else if (doc == null || docVersion == doc.version) {
      termCountInDoc.compute(term, (w, map) -> {
        if (map == null)
          map = new TIntIntHashMap();
        map.adjustOrPutValue(docId, count, count);
        return map;
      });
      if (doc == null)
        docs.put(docId, new Doc(docVersion, count));
      else
        doc.length(doc.length() + count);

      final int newDocsSize = docs.size();
      avgDocsLength = (avgDocsLength * prevDocsSize + count) / (double) newDocsSize;
    }
  }

  @Override
  public int termCountInDoc(String term, int docId) {
    final TIntIntMap map = termCountInDoc.get(term);
    return map == null ? 0 : map.get(docId);
  }

  @Override
  public int docCountWithTerm(String term) {
    final TIntIntMap map = termCountInDoc.get(term);
    return map == null ? 0 : map.size();
  }

  @Override
  public int docLength(int docId) {
    final Doc doc = docs.get(docId);
    return doc == null ? 0 : doc.length();
  }

  @Override
  public double avgDocsLength() {
    return avgDocsLength;
  }

  private static class Doc {
    private int version;
    private int length;

    Doc(int version, int length) {
      this.version = version;
      this.length = length;
    }

    int version() {
      return version;
    }

    void version(int version) {
      this.version = version;
    }

    int length() {
      return length;
    }

    void length(int length) {
      this.length = length;
    }
  }
}
