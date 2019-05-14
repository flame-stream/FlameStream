package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierTfIdf;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;

import java.util.function.Function;
import java.util.stream.Stream;

public class NonLabeledFilter implements Function<TfIdfObject, Stream<ClassifierInput>> {

    @Override
    public Stream<ClassifierInput> apply(TfIdfObject tfidf) {
        if (tfidf.label() == null) {
            return Stream.of(new ClassifierTfIdf(tfidf));
        } else {
            return Stream.of();
        }
    }
}
