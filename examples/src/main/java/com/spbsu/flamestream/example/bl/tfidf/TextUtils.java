package com.spbsu.flamestream.example.bl.tfidf;


import com.spbsu.flamestream.example.bl.tfidf.model.TFData;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier.SklearnSgdPredictor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TextUtils {
  public static List<String> words(String text) {
    return SklearnSgdPredictor.text2words(text).collect(Collectors.toList());
  }

  public static TFData tfData(String text) {
    return TFData.ofWords(words(text));
  }
}
