package com.spbsu.flamestream.example.bl.classifier;

import java.util.Map;

class Document {
    //private final Map<String, Double> tf;
    //private final Map<String, Double> idf;
    private final Map<String, Double> tfidf;

    public Document(Map<String, Double> tfidf) {
      this.tfidf = tfidf;
    }

    public double[] getTfidfRepresentation(Map<Integer, String> inverseCountVectorizer) {
      final double[] res = new double[inverseCountVectorizer.size()];

      for (int i = 0; i < inverseCountVectorizer.size(); i++) {
        final String word = inverseCountVectorizer.get(i);
        res[i] = tfidf.get(word);
      }

      return res;
    }
}