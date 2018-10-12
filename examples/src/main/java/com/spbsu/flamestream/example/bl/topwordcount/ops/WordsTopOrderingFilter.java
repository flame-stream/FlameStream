package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.example.bl.topwordcount.model.WordCounter;

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
      throw new IllegalStateException("The only element in group should be WordCounter");
    }

    if (objects.size() == 1 || (objects.get(0) instanceof CountWordsTop
            && objects.get(1) instanceof WordCounter)) {
      return Stream.of(objects);
    } else {
      return Stream.empty();
    }
  }
}
