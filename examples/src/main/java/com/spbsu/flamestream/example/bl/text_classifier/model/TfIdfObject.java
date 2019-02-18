package com.spbsu.flamestream.example.bl.text_classifier.model;


import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class TfIdfObject implements DocContainer {
  private final int number;
  private final Map<String, Integer> counts;
  private final Map<String, Integer> idfCounts;
  private final String docName;
  private final String partitioning;
  private final int idfCardinality;

  public Set<String> tfKeys() {
    return counts.keySet();
  }

  public int tfCount(String key) {
    return counts.get(key);
  }

  public int idfCount(String key) {
    return idfCounts.get(key);
  }

  private TfIdfObject(String docName, Stream<String> words, String partitioning, int number) {
    this.docName = docName;
    this.number = number;
    this.partitioning = partitioning;
    counts = new HashMap<>();
    idfCounts = new HashMap<>();
    idfCardinality = 0;
    words.forEach(s -> counts.merge(s, 1, Integer::sum));
  }

  public static TfIdfObject ofText(TextDocument textDocument) {
    return new TfIdfObject(
            textDocument.name(),
            SklearnSgdPredictor.text2words(textDocument.content()),
            textDocument.partitioning(),
            textDocument.number()
    );
  }

  public TfIdfObject(TfIdfObject tfIdfObject, Map<String, Integer> idf) {
    this(tfIdfObject.docName, tfIdfObject.counts, idf, tfIdfObject.partitioning, tfIdfObject.number);
  }

  private TfIdfObject(String docName,
                      Map<String, Integer> counts,
                      Map<String, Integer> idfCounts,
                      String partitioning,
                      int number) {
    this.docName = docName;
    this.counts = counts;
    this.idfCounts = idfCounts;
    this.partitioning = partitioning;
    idfCardinality = 0;
    this.number = number;
  }

  @Override
  public String document() {
    return docName;
  }

  public int number() {
    return number;
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
    return String.format(
            "<TFO> doc hash: %d, doc: %s, idf: %s, words: %s",
            docName.hashCode(),
            docName,
            idfCounts,
            counts
    );
  }
}
