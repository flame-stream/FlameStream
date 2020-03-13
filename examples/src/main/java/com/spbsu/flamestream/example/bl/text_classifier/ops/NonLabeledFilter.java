package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.spbsu.flamestream.core.graph.SerializableFunction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;

import java.util.stream.Stream;

public class NonLabeledFilter implements SerializableFunction<TfIdfObject, Stream<ClassifierInput>> {

  @Override
  public Stream<ClassifierInput> apply(TfIdfObject tfidf) {
    if (tfidf.label() == null) {
      return Stream.of(tfidf);
    } else {
      return Stream.of();
    }
  }
}
