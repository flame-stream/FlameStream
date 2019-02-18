package com.spbsu.flamestream.example.bl.text_classifier.model;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;

import java.util.HashMap;
import java.util.Map;

public class IdfObject implements DocContainer {
  private final Map<String, Integer> counts = new HashMap<>();
  private final String docName;
  private final String partitioning;
  private final int idfCardinality;

  public boolean isComplete() {
    return counts.size() == idfCardinality;
  }

  public IdfObject(WordCounter wordCounter) {
    this.docName = wordCounter.document();
    this.partitioning = wordCounter.partitioning();
    counts.put(wordCounter.word(), wordCounter.count());
    this.idfCardinality = wordCounter.idfCardinality();
  }

  public IdfObject(IdfObject some, WordCounter wordCounter) {
    this.docName = some.docName;
    this.partitioning = some.partitioning;
    this.idfCardinality = some.idfCardinality;
    counts.putAll(some.counts);
    counts.put(wordCounter.word(), wordCounter.count());
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
  public String toString() {
    return String.format("<IDFO> %s", counts);
  }

  public Map<String, Integer> counts() {
    return counts;
  }
}
