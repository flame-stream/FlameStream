package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierOutput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierWeights;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.Prediction;

import java.util.function.Function;
import java.util.stream.Stream;

public class WeightsFilter implements Function<ClassifierOutput, Stream<ClassifierInput>> {

    @Override
    public Stream<ClassifierInput> apply(ClassifierOutput output) {
        if (output instanceof Prediction) {
            return Stream.of();
        } else {
            return Stream.of((ClassifierWeights) output);
        }
    }
}