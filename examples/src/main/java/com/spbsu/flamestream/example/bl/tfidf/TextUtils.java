package com.spbsu.flamestream.example.bl.tfidf;


import com.spbsu.flamestream.example.bl.tfidf.model.TFData;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextUtils {
  static List<String> words(String text) {
    Pattern p = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
    Matcher m = p.matcher(text);
    List<String> result = new ArrayList<>();
    while (m.find()) {
      result.add(m.group());
    }
    return result;
  }

  static TFData tfData(String text) {
    return TFData.ofWords(words(text));
  }
}
