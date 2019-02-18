package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

import java.util.HashMap;
import java.util.Map;

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
    counts.putAll(other.counts);
    counts.putAll(some.counts);
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
}
