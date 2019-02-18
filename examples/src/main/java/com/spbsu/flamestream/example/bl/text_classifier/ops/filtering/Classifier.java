package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;

import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class Classifier implements Function<TfIdfObject, Stream<Prediction>> {
  private final TopicsPredictor predictor;

  public Classifier(TopicsPredictor predictor) {
    this.predictor = predictor;
  }

  @Override
  public Stream<Prediction> apply(TfIdfObject tfIdfObject) {
    final Document document = new Document(computeTfIdf(tfIdfObject));

    final Prediction result = new Prediction(tfIdfObject, predictor.predict(document));

    return Stream.of(result);
  }

  public void init() {
    predictor.init();
  }

  private Map<String, Double> computeTfIdf(TfIdfObject tfIdfObject) {
    final Map<String, Double> tfIdf = new HashMap<>();
    double squareSum = 0.0;
    for (String word : tfIdfObject.tfKeys()) {
      double tfIdfValue =
              tfIdfObject.tfCount(word) * Math.log((double) tfIdfObject.number() / (double) tfIdfObject.idfCount(word))
                      + 1;
      squareSum += (tfIdfValue * tfIdfValue);
      tfIdf.put(word, tfIdfValue);
    }

    final double norm = Math.sqrt(squareSum);
    tfIdf.forEach((s, v) -> tfIdf.put(s, v / norm));
    return tfIdf;
  }

}
