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
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLProximal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class ClassifierFilter implements Function<List<ClassifierInput>, Stream<ClassifierOutput>> {
  private final Vectorizer vectorizer;
  private final ModelState defaultState;
  private final FTRLProximal optimizer;

  public ClassifierFilter(Vectorizer vectorizer,
                          ModelState defaultState,
                          FTRLProximal optimizer) {
    this.vectorizer = vectorizer;
    this.defaultState = defaultState;
    this.optimizer = optimizer;
  }

  private Stream<ClassifierOutput> processTfIdf(ClassifierInput input) {
    TfIdfObject tfIdfObject = ((ClassifierTfIdf) input).getTfidf();

    if (tfIdfObject.label() != null) {
      DataPoint point = new DataPoint(vectorize(tfIdfObject), tfIdfObject.label());
      ModelState firstState = optimizer.step(point, defaultState);
      return Stream.of(new ClassifierState(firstState));
    } else {
      final Prediction result = new Prediction(tfIdfObject, new Topic[1]);
      //TopicsPredictor.predict(tfIdfObject, vectorize(tfIdfObject)));
      return Stream.of(result);
    }
  }

  @Override
  public Stream<ClassifierOutput> apply(List<ClassifierInput> input) {
    if (input.size() == 1) {
      return processTfIdf(input.get(0));
    }

    if (input.get(0) instanceof ClassifierState) {
      //return TopicsPredictor.predict(input.get(0), vectorize(input.get(1)));
      TfIdfObject tfIdfObject = ((ClassifierTfIdf) input.get(1)).getTfidf();
      return Stream.of(new Prediction(tfIdfObject, new Topic[2]));
    } else {
      return Stream.empty();
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
