package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.expleague.commons.math.vectors.Vec;
import com.spbsu.flamestream.example.bl.text_classifier.model.ClassifierState;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierOutput;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.DataPoint;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.OnlineModel;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class ClassifierFilter implements Function<List<ClassifierInput>, Stream<ClassifierOutput>> {
  private final static Logger LOG = LoggerFactory.getLogger(ClassifierFilter.class);
  private final Vectorizer vectorizer;
  private final OnlineModel model;

  public ClassifierFilter(Vectorizer vectorizer, OnlineModel model) {
    this.vectorizer = vectorizer;
    this.model = model;
  }

  public void init() {
    vectorizer.init();
  }

  @Override
  public Stream<ClassifierOutput> apply(List<ClassifierInput> input) {
    if (input.size() == 1) {
      final TfIdfObject tfIdfObject = (TfIdfObject) input.get(0);
      if (tfIdfObject.label() == null) {
        LOG.warn("Cannot process doc: {}. Empty model.", tfIdfObject.document());
        return Stream.empty();
      }
      final ModelState firstState = model.step(
              new DataPoint(vectorize(tfIdfObject), tfIdfObject.label()),
              new FTRLState(model.classes(), vectorizer.dim())
      );
      return Stream.of(new Prediction(tfIdfObject, new Topic[]{}), new ClassifierState(firstState));
    }

    if (input.get(0) instanceof ClassifierState) {
      final TfIdfObject tfIdfObject = (TfIdfObject) input.get(1);
      final Vec features = vectorize(tfIdfObject);
      final ModelState state = ((ClassifierState) input.get(0)).getState();
      if (tfIdfObject.label() == null) {
        final Topic[] prediction = model.predict(state, features);
        return Stream.of(new Prediction(tfIdfObject, prediction), (ClassifierState) input.get(0));
      } else {
        final ModelState newState = model.step(new DataPoint(features, tfIdfObject.label()), state);
        return Stream.of(new Prediction(tfIdfObject, new Topic[]{}), new ClassifierState(newState));
      }
    } else {
      return Stream.of();
    }
  }

  private Vec vectorize(TfIdfObject tfIdfObject) {
    final Map<String, Double> tfIdf = new HashMap<>();
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
