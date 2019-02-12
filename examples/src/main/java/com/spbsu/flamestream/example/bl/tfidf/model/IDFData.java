package com.spbsu.flamestream.example.bl.tfidf.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IDFData {
  private Map<String, Integer> data;

  public IDFData() {
    data = new HashMap<>();
  }

  public void addWords(Collection<String> words) {
    for (String w: words) {
      data.put(w, data.getOrDefault(w, 0) + 1);
    }
  }

  public Set<String> keys() {
    return data.keySet();
  }

  public int value(String key) {
    return data.get(key);
  }
}
