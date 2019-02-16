package com.spbsu.flamestream.example.bl.tfidf;


import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier.SklearnSgdPredictor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TextUtils {
  public static List<String> words(String text) {
    return wordsStream(text).collect(Collectors.toList());
  }

  public static Stream<String> wordsStream(String text) {
    return SklearnSgdPredictor.text2words(text);
  }

  public static Set<String> wordsSet(String text) {
    return wordsStream(text).collect(Collectors.toSet());
  }


  public static Map<String, Integer> tfData(String text) {
    Map<String, Integer> result = new HashMap<>();
    wordsStream(text).forEach(w -> result.merge(w, 1, Integer::sum));
    return result;
  }
}
