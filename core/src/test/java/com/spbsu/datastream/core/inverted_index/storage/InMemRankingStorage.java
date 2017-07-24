package com.spbsu.datastream.core.inverted_index.storage;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.*;

/**
 * User: Artem
 * Date: 11.07.2017
 */
public class InMemRankingStorage implements RankingStorage {
  private final Set<Document> allDocs = new HashSet<>();
  private final Map<String, TIntIntMap> termCountInDoc = new HashMap<>();

  private final TIntIntMap docsLength = new TIntIntHashMap();
  private final TIntIntMap docsVersion = new TIntIntHashMap();
  private double avgDocsLength = 0.0;

  @Override
  public void add(String term, int count, Document document) {
    final int prevDocsSize = docsLength.size();
    final boolean prevVersionExists = allDocs.contains(document);
    if (prevVersionExists && document.version() > docsVersion.get(document.id())) { //uncommon scenario
      termCountInDoc.forEach((t, map) -> {
        if (t.equals(term))
          map.put(document.id(), count);
        else
          map.remove(document.id());
      });
      { //length
        avgDocsLength = (avgDocsLength * prevDocsSize - docLength(document.id()) + count) / (double) prevDocsSize;
        docsLength.put(document.id(), count);
      }
      { //all docs & version
        docsVersion.put(document.id(), document.version());
        allDocs.remove(document);
        allDocs.add(document);
      }
    } else if (!prevVersionExists || document.version() == docsVersion.get(document.id())) {
      termCountInDoc.compute(term, (w, map) -> {
        if (map == null)
          map = new TIntIntHashMap();
        map.adjustOrPutValue(document.id(), count, count);
        return map;
      });
      { //all docs & version
        if (!prevVersionExists)
          docsVersion.put(document.id(), document.version());
        allDocs.add(document);
      }
      { //length
        docsLength.adjustOrPutValue(document.id(), count, count);
        final int newDocsSize = docsLength.size();
        avgDocsLength = (avgDocsLength * prevDocsSize + count) / (double) newDocsSize;
      }
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
    return docsLength.get(docId);
  }

  @Override
  public double avgDocsLength() {
    return avgDocsLength;
  }

  @Override
  public Collection<Document> allDocs() {
    return allDocs;
  }
}
