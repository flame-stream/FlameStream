package com.spbsu.flamestream.example.bl.tfidf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class IdfReader {
  private static final String PATH_TO_IDF = "src/test/resources/idfs";

  static Map<String, Double> readIdf() {
    Map<String, Double> idf = new HashMap<>();

    try (BufferedReader br = new BufferedReader(new FileReader(new File(PATH_TO_IDF)))) {
      String[] items = br.readLine().split(" ");
      String word = items[0];
      double idfFeature = Double.parseDouble(items[1]);

      idf.put(word, idfFeature);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return idf;
  }
}
