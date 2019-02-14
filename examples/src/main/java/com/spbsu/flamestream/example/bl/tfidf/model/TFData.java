package com.spbsu.flamestream.example.bl.tfidf.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TFData {
  private Map<String, Integer> data;

  private TFData(List<String> words) {
    data = new HashMap<>();
    for (String w: words) {
      data.put(w, data.getOrDefault(w, 0) + 1);
    }
  }

  public static TFData ofWords(List<String> words) {
    return new TFData(words);
  }

  public Set<String> keys() {
    return data.keySet();
  }

  public int value(String key) {
    return data.get(key);
  }
}
