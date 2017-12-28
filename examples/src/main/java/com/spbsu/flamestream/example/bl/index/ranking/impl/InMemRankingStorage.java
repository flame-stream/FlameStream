package com.spbsu.flamestream.example.bl.index.ranking.impl;

import com.spbsu.flamestream.example.bl.index.ranking.Document;
import com.spbsu.flamestream.example.bl.index.ranking.RankingStorage;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * User: Artem
 * Date: 11.07.2017
 */
public class InMemRankingStorage implements RankingStorage {
  private final Map<CharSequence, TIntIntMap> termCountInDoc = new HashMap<>();
  private final TIntIntMap docsLength = new TIntIntHashMap();
  private final TIntIntMap docsVersion = new TIntIntHashMap();
  private double avgDocsLength = 0.0;

  @Override
  public void add(CharSequence term, int count, Document document) {
    final int prevDocsSize = docsLength.size();
    final boolean prevVersionExists = docsVersion.containsKey(document.id());
    final int prevDocVersion = docsVersion.get(document.id());
    if (prevVersionExists && document.version() > prevDocVersion) { //uncommon scenario
      termCountInDoc.forEach((t, map) -> {
        if (t.equals(term)) {
          map.put(document.id(), count);
        } else {
          map.remove(document.id());
        }
      });
      avgDocsLength = (avgDocsLength * prevDocsSize - docsLength.get(document.id()) + count) / (double) prevDocsSize;
      docsLength.put(document.id(), count);
      docsVersion.put(document.id(), document.version());
    } else if (!prevVersionExists || document.version() == prevDocVersion) {
      termCountInDoc.compute(term, (w, map) -> {
        final TIntIntMap realMap = map == null ? new TIntIntHashMap() : map;
        realMap.adjustOrPutValue(document.id(), count, count);
        return realMap;
      });
      if (!prevVersionExists) {
        docsVersion.put(document.id(), document.version());
      }
      docsLength.adjustOrPutValue(document.id(), count, count);
      final int newDocsSize = docsLength.size();
      avgDocsLength = (avgDocsLength * prevDocsSize + count) / (double) newDocsSize;
    }
  }

  @Override
  public int termCountInDoc(CharSequence term, Document document) {
    if (!docsVersion.containsKey(document.id())) {
      throw new IllegalArgumentException("Storage does not contain requested doc");
    }

    if (docsVersion.get(document.id()) != document.version()) {
      throw new IllegalArgumentException("Storage does not contain doc with requested version");
    }
    final TIntIntMap map = termCountInDoc.get(term);
    return map == null ? 0 : map.get(document.id());
  }

  @Override
  public int docCountWithTerm(CharSequence term) {
    final TIntIntMap map = termCountInDoc.get(term);
    return map == null ? 0 : map.size();
  }

  @Override
  public int docLength(Document document) {
    if (!docsVersion.containsKey(document.id())) {
      throw new IllegalArgumentException("Storage does not contain requested doc");
    }

    if (docsVersion.get(document.id()) != document.version()) {
      throw new IllegalArgumentException("Storage does not contain doc with requested version");
    }
    return docsLength.get(document.id());
  }

  @Override
  public double avgDocsLength() {
    return avgDocsLength;
  }

  @Override
  public Stream<Document> allDocs() {
    final TIntIntIterator iterator = docsVersion.iterator();
    final Iterable<Document> iterable = () -> new Iterator<Document>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Document next() {
        iterator.advance();
        return new Document(iterator.key(), iterator.value());
      }
    };
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  @Override
  public int docsCount() {
    return docsLength.size();
  }
}
