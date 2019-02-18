package com.spbsu.flamestream.example.bl.text_classifier.ops.entries;

import com.spbsu.flamestream.example.bl.text_classifier.model.IdfObject;
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
      return Stream.of(new IdfObject(wordCounter));
    } else {
      final IdfObject idfObject = (IdfObject) docContainers.get(0);
      final WordCounter wordCounter = (WordCounter) docContainers.get(1);
      final IdfObject result = new IdfObject(idfObject, wordCounter);
      return Stream.of(result);
    }
  }
}