package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.expleague.commons.math.vectors.Vec;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.*;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.CountVectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.DataPoint;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.TextUtils;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.TopicsPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLProximal;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class ClassifierFilter implements Function<List<ClassifierInput>, Stream<ClassifierOutput>> {
  private final TopicsPredictor predictor;
  private final String cntVectorizerPath = "src/main/resources/cnt_vectorizer";
  private final Vectorizer vectorizer = new CountVectorizer(cntVectorizerPath);
  private final String topicsPath = "src/main/resources/topics";
  private final FTRLProximal updater = new FTRLProximal.Builder().build(TextUtils.readTopics(topicsPath));

  public ClassifierFilter(TopicsPredictor predictor) {
    this.predictor = predictor;
  }

  private Stream<ClassifierOutput> processTfIdf(ClassifierInput input) {
    TfIdfObject tfIdfObject = ((ClassifierTfIdf) input).getTfidf();

    if (tfIdfObject.label() != null) {
      DataPoint point = new DataPoint(vectorize(tfIdfObject), tfIdfObject.label());
      ModelState newState = updater.step(point, predictor.getState());
      return Stream.of(new ClassifierState(newState));
    } else {
      final Prediction result = predict(tfIdfObject);
      return Stream.of(result);
    }
  }

  @Override
  public Stream<ClassifierOutput> apply(List<ClassifierInput> input) {
    if (input.size() == 1) {
      return processTfIdf(input.get(0));
    }

    if (input.get(0) instanceof ClassifierState) {
      return processWithNewWeights(input.get(0), input.get(1));
    } else {
      return Stream.empty();
    }

  }

  private Stream<ClassifierOutput> processWithNewWeights(ClassifierInput state, ClassifierInput tfidf) {
    predictor.updateState(((ClassifierState) state).getState());
    return processTfIdf(tfidf);
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

  private Prediction predict(TfIdfObject tfIdfObject) {
    return new Prediction(tfIdfObject,
            predictor.predict(vectorize(tfIdfObject)));
  }

  public void init() {
    predictor.init();
  }
}
