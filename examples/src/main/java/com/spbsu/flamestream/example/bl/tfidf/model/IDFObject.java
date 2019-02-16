package com.spbsu.flamestream.example.bl.tfidf.model;

import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordCounter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class IDFObject implements DocContainer {
  private final Map<String, Integer> counts = new HashMap<>();
  private final String docName;
  private final String partitioning;
  private final int idfCardinality;

  public boolean isComplete() {
    return counts.size() == idfCardinality;
  }

  public IDFObject(WordCounter wordCounter) {
    this.docName = wordCounter.document();
    this.partitioning = wordCounter.partitioning();
    counts.put(wordCounter.word(), wordCounter.count());
    this.idfCardinality = wordCounter.idfCardinality();
  }

  private IDFObject(String docName, int idfCardinality, String partitioning) {
    this.docName = docName;
    this.partitioning = partitioning;
    this.idfCardinality = idfCardinality;
  }

  public IDFObject(IDFObject some, IDFObject other) {
    this(some.docName, some.idfCardinality, some.partitioning);
    for (Map.Entry<String, Integer> entry: other.counts.entrySet()) {
      counts.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Integer> entry: some.counts.entrySet()) {
      counts.put(entry.getKey(), entry.getValue());
    }
  }


  @Override
  public String document() {
    return docName;
  }

  @Override
  public String partitioning() {
    return partitioning;
  }

  @Override
  public int idfCardinality() {
    return idfCardinality;
  }

  @Override
  public String toString() {
    return String.format("<IDFO> %s", counts);
  }

  public Map<String, Integer> counts() {
    return counts;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final IDFObject otherIDFObject = (IDFObject) o;
    return Objects.equals(counts, otherIDFObject.counts);
  }

  @Override
  public int hashCode() {
    return Objects.hash(counts.hashCode());
  }

}
