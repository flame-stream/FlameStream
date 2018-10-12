package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.example.bl.topwordcount.model.WordCounter;
import com.spbsu.flamestream.example.bl.topwordcount.model.WordsTop;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class WordsTopOrderingFilter implements Function<List<Object>, Stream<List<Object>>> {
  @Override
  public Stream<List<Object>> apply(List<Object> objects) {
    if (objects.size() > 2) {
      throw new IllegalStateException("Group size should be <= 2");
    }

    if (objects.size() == 1 && !(objects.get(0) instanceof WordCounter)) {
      return Stream.empty();
    }

    if (objects.size() == 1 || (objects.get(0) instanceof WordsTop
            && objects.get(1) instanceof WordCounter)) {
      //System.out.println(objects);
      return Stream.of(objects);
    } else {
      return Stream.empty();
    }
  }
}
