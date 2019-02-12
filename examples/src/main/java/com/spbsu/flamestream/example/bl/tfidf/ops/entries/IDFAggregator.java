package com.spbsu.flamestream.example.bl.tfidf.ops.entries;

import com.spbsu.flamestream.example.bl.tfidf.model.IDFObject;
import com.spbsu.flamestream.example.bl.tfidf.model.containers.DocContainer;
import com.spbsu.flamestream.example.bl.tfidf.model.counters.WordCounter;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

public class IDFAggregator implements Function<List<DocContainer>, Stream<DocContainer>> {
  @Override
  public Stream<DocContainer> apply(List<DocContainer> docContainers) {
    if (docContainers.size() == 1) {
      final WordCounter wordCounter = (WordCounter) docContainers.get(0);
      return Stream.of(new IDFObject(wordCounter.document(), wordCounter.word(), wordCounter.count(), wordCounter.idfCardinality(), wordCounter.partitioning()));
    } else {
      final IDFObject counter = (IDFObject) docContainers.get(0);
      final WordCounter wordCounter = (WordCounter) docContainers.get(1);
      IDFObject result = counter.merge(new IDFObject(wordCounter.document(), wordCounter.word(), wordCounter.count(), wordCounter.idfCardinality(), wordCounter.partitioning()));
      return Stream.of(result);
    }
  }
}