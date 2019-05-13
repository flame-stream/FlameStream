package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TextUtils {
  private static final Pattern PATTERN = Pattern.compile("\\b\\w\\w+\\b", Pattern.UNICODE_CHARACTER_CLASS);

  public static Stream<String> text2words(String text) {
    final Matcher matcher = PATTERN.matcher(text);
    final Iterable<String> iterable = () -> new Iterator<String>() {
      @Override
      public boolean hasNext() {
        return matcher.find();
      }

      @Override
      public String next() {
        return matcher.group(0);
      }
    };
    return StreamSupport.stream(iterable.spliterator(), false);
  }
}
