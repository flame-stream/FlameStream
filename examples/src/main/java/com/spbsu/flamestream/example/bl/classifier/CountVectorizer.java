package com.spbsu.flamestream.example.bl.classifier;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

class CountVectorizer implements Vectorizer {
  private final String cntVectorizerPath;
  private TObjectIntMap<String> countVectorizer;

  CountVectorizer(String cntVectorizerPath) {
    this.cntVectorizerPath = cntVectorizerPath;
  }

  private void loadVocabulary() {
    final File countFile = new File(cntVectorizerPath);
    countVectorizer = new TObjectIntHashMap<>();

    try (final BufferedReader countFileReader = new BufferedReader(new FileReader(countFile))) {

      String line;
      while ((line = countFileReader.readLine()) != null) {
        final String[] items = line.split(" ");
        final String key = items[1];
        final int value = Integer.parseInt(items[0]);
        countVectorizer.put(key, value);
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

    final double[] res = new double[countVectorizer.size()];
    final TObjectDoubleMap<String> tfidf = document.getTfidf();
    tfidf.forEachEntry((s, v) -> {
      res[countVectorizer.get(s)] = v;
      return true;
    });

    return res;
  }

  public int vectorize(String word) {
    if (countVectorizer == null) {
      loadVocabulary();
    }

    return countVectorizer.get(word);
  }
}
