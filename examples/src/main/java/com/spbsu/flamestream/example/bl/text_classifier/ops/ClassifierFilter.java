package com.spbsu.flamestream.example.bl.text_classifier.ops;

import com.expleague.commons.math.vectors.Vec;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierInput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierOutput;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierState;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.ClassifierTfIdf;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.CountVectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.DataPoint;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Vectorizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLProximal;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl.FTRLState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class ClassifierFilter implements Function<List<ClassifierInput>, Stream<ClassifierOutput>> {
  private final Vectorizer vectorizer;
  private final FTRLProximal optimizer;
  private final int rows;
  private final int columns;

  public ClassifierFilter(String cntVectorizerPath, String[] topics) {
    final CountVectorizer countVectorizer = new CountVectorizer(cntVectorizerPath);
    countVectorizer.init();

    this.vectorizer = countVectorizer;
    this.optimizer = new FTRLProximal.Builder().build(topics);
    this.rows = topics.length;
    this.columns = countVectorizer.size();
  }

  private Stream<ClassifierOutput> processTfIdf(ClassifierInput input) {
    TfIdfObject tfIdfObject = ((ClassifierTfIdf) input).getTfidf();
    ModelState initialState = new FTRLState(rows, columns);

    if (tfIdfObject.label() != null) {
      DataPoint point = new DataPoint(vectorize(tfIdfObject), tfIdfObject.label());
      ModelState firstState = optimizer.step(point, initialState);
      return Stream.of(new ClassifierState(firstState));
    } else {
      Vec features = vectorize(tfIdfObject);
      final Prediction result = new Prediction(tfIdfObject,
              new ClassifierState(initialState), optimizer.predict(initialState, features));
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
      Topic[] prediction = optimizer.predict(state, features);

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
