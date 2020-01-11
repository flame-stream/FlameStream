package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierOutput;
import com.spbsu.flamestream.example.bl.text_classifier.model.ClassifierState;

import java.util.stream.Stream;

public class WeightsFilter implements FlameMap.SerializableFunction<ClassifierOutput, Stream<ClassifierInput>> {

  @Override
  public Stream<ClassifierInput> apply(ClassifierOutput output) {
    if (output instanceof ClassifierState) {
      return Stream.of((ClassifierState) output);
    }
    return Stream.empty();
  }
}