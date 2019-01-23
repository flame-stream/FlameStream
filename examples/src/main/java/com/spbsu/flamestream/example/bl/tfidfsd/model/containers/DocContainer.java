package com.spbsu.flamestream.example.bl.tfidfsd.model.containers;

public interface DocContainer {
    String document();
    String partitioning();
    int idfCardinality();
}