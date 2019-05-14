package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.expleague.commons.math.vectors.Vec;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.TopicsPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class ClassifierFilter implements Function<TfIdfObject, Stream<Prediction>> {
  private final Vectorizer vectorizer;
  private final TopicsPredictor predictor;

  public ClassifierFilter(Vectorizer vectorizer, TopicsPredictor predictor) {
    this.vectorizer = vectorizer;
    this.predictor = predictor;
  }

  @Override
  public Stream<Prediction> apply(TfIdfObject tfIdfObject) {
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

    final Document document = new Document(tfIdf);
    final Vec vec = vectorizer.vectorize(document);
    final Prediction result = new Prediction(tfIdfObject, predictor.predict(vec));
    return Stream.of(result);
  }

  public void init() {
    vectorizer.init();
    predictor.init();
  }
}