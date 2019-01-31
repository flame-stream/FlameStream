package com.spbsu.flamestream.example.bl.classifier;

import gnu.trove.map.TObjectDoubleMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

class CountVectorizer implements Vectorizer {
  private final String cntVectorizerPath;
  private final int features;
  private String[] countVectorizer;

  CountVectorizer(String cntVectorizerPath, int features) {
    this.cntVectorizerPath = cntVectorizerPath;
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

  @Override
  public double[] vectorize(Document document) {
    if (countVectorizer == null) {
      loadVocabulary();
    }

    final double[] res = new double[countVectorizer.length];
    final TObjectDoubleMap<String> tfidf = document.getTfidf();

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
