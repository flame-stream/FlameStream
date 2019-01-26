package com.spbsu.flamestream.example.bl.classifier;

class Document {
    private final double[] tfidf;

    public Document(double[] tfidf) {
        this.tfidf = tfidf;
    }

    public double[] getTfidfRepresentation() {
        return tfidf;
    }
}