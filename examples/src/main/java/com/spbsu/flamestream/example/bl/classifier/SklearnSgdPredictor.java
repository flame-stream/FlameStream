package com.spbsu.flamestream.example.bl.classifier;

import org.jblas.DoubleMatrix;
import org.jblas.MatrixFunctions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static org.jblas.MatrixFunctions.exp;

public class SklearnSgdPredictor implements TopicsPredictor {
  private final String weightsPath;
  private final String cntVectorizerPath;
  private CountVectorizer vectorizer;
  private double[] intercept;
  private DoubleMatrix weights;
  private String[] topics;

  SklearnSgdPredictor(String cntVectorizerPath, String weightsPath) {
    this.weightsPath = weightsPath;
    this.cntVectorizerPath = cntVectorizerPath;
  }

  private void loadMeta() {
    final File metaData = new File(weightsPath);

    try (final BufferedReader br = new BufferedReader(new FileReader(metaData))) {
      final double[] meta = parseDoubles(br.readLine());
      final int classes = (int) meta[0];
      final int currentFeatures = (int) meta[1];
      vectorizer = new CountVectorizer(cntVectorizerPath, currentFeatures);
      topics = new String[classes];
      for (int i = 0; i < classes; i++) {
        topics[i] = br.readLine();
      }

      final double[][] inputCoef = new double[classes][currentFeatures];
      String line;
      for (int index = 0; index < classes; index++) {
        line = br.readLine();
        final double[] numbers = parseDoubles(line);

        assert numbers.length == currentFeatures;
        inputCoef[index] = numbers;
      }

      line = br.readLine();
      intercept = parseDoubles(line);
      weights = new DoubleMatrix(inputCoef).transpose();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Topic[] predict(Document document) {
    if (weights == null) {
      loadMeta();
    }

    final double[] vectorized = vectorizer.vectorize(document);
    final DoubleMatrix probs = predictProba(new DoubleMatrix(vectorized));
    final Topic[] result = new Topic[probs.length];

    for (int index = 0; index < probs.data.length; index++) {
      result[index] = new Topic(topics[index], Integer.toString(index), probs.data[index]);
    }

    return result;
  }

  static double[] parseDoubles(String line) {
    return Arrays
            .stream(line.split(" "))
            .mapToDouble(Double::parseDouble)
            .toArray();
  }

  // see _predict_proba_lr in base.py sklearn
  private DoubleMatrix predictProba(DoubleMatrix documents) {
    DoubleMatrix probabilities = decisionFunction(documents);

    probabilities = probabilities.mul(-1);
    probabilities = exp(probabilities);
    probabilities.add(1);
    probabilities = MatrixFunctions.powi(probabilities, -1);

    final double[][] matrix = new double[1][probabilities.rows];
    matrix[0] = probabilities.rowSums().data;
    final DoubleMatrix denominator = new DoubleMatrix(matrix);
    denominator.reshape(probabilities.rows, probabilities.rows);

    return probabilities.div(denominator);
  }

  private DoubleMatrix decisionFunction(DoubleMatrix documents) {
    final DoubleMatrix inter = new DoubleMatrix(1, intercept.length, intercept);
    final DoubleMatrix score = documents.transpose().mmul(weights);
    return score.add(inter);
  }
}
