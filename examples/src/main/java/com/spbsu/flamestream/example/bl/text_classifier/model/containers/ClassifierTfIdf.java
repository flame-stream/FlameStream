package com.spbsu.flamestream.example.bl.text_classifier.model.containers;

import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;

public class ClassifierTfIdf implements ClassifierInput {
    private final TfIdfObject tfidf;

    public ClassifierTfIdf(TfIdfObject tfidf) {
        this.tfidf = tfidf;
    }

    public TfIdfObject getTfidf() {
        return tfidf;
    }
}
