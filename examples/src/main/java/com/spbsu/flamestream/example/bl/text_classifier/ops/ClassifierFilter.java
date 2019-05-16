package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.expleague.commons.math.vectors.Vec;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierOutput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierState;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierTfIdf;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.DataPoint;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.OnlineModel;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class ClassifierFilter implements Function<List<ClassifierInput>, Stream<ClassifierOutput>> {
  private final Vectorizer vectorizer;
  private final OnlineModel model;

  public ClassifierFilter(Vectorizer vectorizer, OnlineModel model) {
    this.vectorizer = vectorizer;
    this.model = model;
  }

  public void init() {
    vectorizer.init();
  }

  private Stream<ClassifierOutput> processTfIdf(ClassifierInput input) {
    final TfIdfObject tfIdfObject = ((ClassifierTfIdf) input).getTfidf();
    final ModelState initialState = new FTRLState(model.classes(), vectorizer.dim());
    if (tfIdfObject.label() != null) {
      DataPoint point = new DataPoint(vectorize(tfIdfObject), tfIdfObject.label());
      ModelState firstState = model.step(point, initialState);
      return Stream.of(new ClassifierState(firstState));
    } else {
      Vec features = vectorize(tfIdfObject);
      final Prediction result = new Prediction(tfIdfObject,
              new ClassifierState(initialState), model.predict(initialState, features)
      );
      return Stream.of(result);
    }
  }

  @Override
  public Stream<ClassifierOutput> apply(List<ClassifierInput> input) {
    if (input.size() == 1) {
      return processTfIdf(input.get(0));
    }

    if (input.get(0) instanceof ClassifierState) {
      TfIdfObject tfIdfObject = ((ClassifierTfIdf) input.get(1)).getTfidf();
      ModelState state = ((ClassifierState) input.get(0)).getState();
      Vec features = vectorize(tfIdfObject);
      Topic[] prediction = model.predict(state, features);

      return Stream.of(new Prediction(tfIdfObject, (ClassifierState) input.get(0), prediction));
    } else {
      return Stream.of();
    }

  }

  private Vec vectorize(TfIdfObject tfIdfObject) {
    final Map<String, Double> tfIdf = new HashMap<>();
    // TODO: 13.05.19 move normalization logic to extra vertex
    { //normalized tf-idf
      double squareSum = 0.0;
      for (String word : tfIdfObject.words()) {
        double tfIdfValue =
                tfIdfObject.tf(word) * Math.log((double) tfIdfObject.number() / (double) tfIdfObject.idf(word))
                        + 1;
        squareSum += (tfIdfValue * tfIdfValue);
        tfIdf.put(word, tfIdfValue);
      }
      final double norm = Math.sqrt(squareSum);
      tfIdf.forEach((s, v) -> tfIdf.put(s, v / norm));
    }

    return vectorizer.vectorize(new Document(tfIdf));
  }
}
