package com.spbsu.flamestream.example.bl.tfidf.model;

import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class IDFObject implements DocContainer {
  private final Map<String, Integer> counts = new ConcurrentHashMap<>();
  private final String docName;
  private final String partitioning;
  private AtomicBoolean selfGrouped;
  private final int idfCardinality;

  public boolean isComplete() {
    return counts.size() == idfCardinality;
  }

  public IDFObject(String docName, String word, Integer count, int idfCardinality, String partitioning) {
    this.docName = docName;
    this.partitioning = partitioning;
    counts.put(word, count);
    selfGrouped = new AtomicBoolean(false);
    this.idfCardinality = idfCardinality;
  }

  private IDFObject(String docName, int idfCardinality, String partitioning) {
    this.docName = docName;
    this.partitioning = partitioning;
    selfGrouped = new AtomicBoolean(false);
    this.idfCardinality = idfCardinality;
  }

  public IDFObject merge(IDFObject other) {
    IDFObject result = new IDFObject(docName, idfCardinality, partitioning);
    for (Map.Entry<String, Integer> entry: other.counts.entrySet()) {
      result.counts.put(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, Integer> entry: counts.entrySet()) {
      result.counts.put(entry.getKey(), entry.getValue());
    }
    return result;
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

  public Set<String> keys() {
    return counts.keySet();
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
