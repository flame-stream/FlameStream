package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.*;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.TopicsPredictor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class ClassifierFilter implements Function<List<ClassifierInput>, Stream<ClassifierOutput>> {
  private final TopicsPredictor predictor;

  public ClassifierFilter(TopicsPredictor predictor) {
    this.predictor = predictor;
  }

  private Stream<ClassifierOutput> processTfIdf(ClassifierInput input) {
    TfIdfObject tfIdfObject = ((ClassifierTfIdf) input).getTfidf();

    if (tfIdfObject.label() != null) {
      Mx newWeights = new SparseMx(1, 1); // change to new classifier weights
      System.out.println("SENDING NEW WEIGHTS");
      return Stream.of(new ClassifierWeights("New weights"));
    } else {
      final Prediction result = new Prediction(tfIdfObject, new Topic[10]);//predict(tfIdfObject);
      return Stream.of(result);
    }
  }

  @Override
  public Stream<ClassifierOutput> apply(List<ClassifierInput> input) {
    if (input.size() == 1) {
      return processTfIdf(input.get(0));
    }

    if (input.get(0) instanceof ClassifierWeights) {
      return processWithNewWeights(input.get(0), input.get(1));
    } else {
      return Stream.empty();
    }

  }

  private Stream<ClassifierOutput> processWithNewWeights(ClassifierInput weights, ClassifierInput tfidf) {
    String newWeights = ((ClassifierWeights) weights).getWeights();

    // update weights of the classifier
    System.out.println("CLASSIFIER UPDATING ITS WEIGHTS");

    return processTfIdf(tfidf);
  }

  private Prediction predict(TfIdfObject tfIdfObject) {
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

    final Document document = new Document(tfIdf);
    System.out.println("CLASSIFIER PREDICTS");
    return new Prediction(tfIdfObject, predictor.predict(document));
  }

  public void init() {
    predictor.init();
  }
}
