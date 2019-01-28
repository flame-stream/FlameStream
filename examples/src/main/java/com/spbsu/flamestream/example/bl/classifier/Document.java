package com.spbsu.flamestream.example.bl.classifier;

import java.util.ArrayList;
import java.util.Map;

class Document {
    private final Map<String, Double> tfidf;

    public Document(Map<String, Double> tfidf) {
      this.tfidf = tfidf;
    }

    public double[] getTfidfRepresentation(ArrayList<String> countVectorizer) {
      final double[] res = new double[countVectorizer.size()];


      for (int i = 0; i < countVectorizer.size(); i++) {
        final String word = countVectorizer.get(i);

        if (tfidf.containsKey(word)) {
          res[i] = tfidf.get(word);
        } else {
          res[i] = 0;
        }
      }

      return res;
    }
}