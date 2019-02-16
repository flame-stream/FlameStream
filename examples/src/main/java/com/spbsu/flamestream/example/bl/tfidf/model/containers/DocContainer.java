package com.spbsu.flamestream.example.bl.tfidf.model.containers;

public interface DocContainer {
  String document();
  String partitioning();
  int idfCardinality();
}
