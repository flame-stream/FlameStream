package com.spbsu.flamestream.example.bl.classifier;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class CountVectorizer {
  private final String cntVectorizerPath = "src/main/resources/cnt_vectorizer";
  private String[] countVectorizer;
  private final int features;

  public CountVectorizer(int features) {
    this.features = features;
  }

  private void loadVocabulary() {
    final File countFile = new File(cntVectorizerPath);
    countVectorizer = new String[features];

    try (final BufferedReader countFileReader = new BufferedReader(new FileReader(countFile))) {

      String line;
      while ((line = countFileReader.readLine()) != null) {
        final String[] items = line.split(" ");
        final String key = items[0];
        final int value = Integer.parseInt(items[1]);
        countVectorizer[value] = key;
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public double[] vectorize(Document document) {
    if (countVectorizer == null) {
      loadVocabulary();
    }

    final double[] res = new double[countVectorizer.length];
    final Map<String, Double> tfidf = document.getTfidf();

    for (int i = 0; i < countVectorizer.length; i++) {
      final String word = countVectorizer[i];

      if (tfidf.containsKey(word)) {
        res[i] = tfidf.get(word);
      } else {
        res[i] = 0;
      }
    }

    return res;
  }
}
