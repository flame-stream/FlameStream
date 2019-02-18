package com.spbsu.flamestream.example.bl.text_classifier.ops.entries;

import com.spbsu.flamestream.example.bl.text_classifier.model.IDFObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.text_classifier.model.WordCounter;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class IDFAggregator implements Function<List<DocContainer>, Stream<DocContainer>> {
  @Override
  public Stream<DocContainer> apply(List<DocContainer> docContainers) {
    if (docContainers.size() == 1) {
      final WordCounter wordCounter = (WordCounter) docContainers.get(0);
      return Stream.of(new IDFObject(wordCounter));
    } else {
      final IDFObject counter = (IDFObject) docContainers.get(0);
      final WordCounter wordCounter = (WordCounter) docContainers.get(1);
      IDFObject result = new IDFObject(counter, new IDFObject(wordCounter));
      return Stream.of(result);
    }
  }
}