package com.spbsu.flamestream.example.bl.classifier;

import org.jblas.DoubleMatrix;
import org.jblas.MatrixFunctions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static org.jblas.MatrixFunctions.exp;

public class Predictor implements TopicsPredictor {
  private final String matrixPath = "src/main/resources/meta_data";
  private double[] intercept;
  private DoubleMatrix weights;

  public Predictor() {}

  private void loadMeta() {
    final File metaData = new File(matrixPath);

    try (final BufferedReader br = new BufferedReader(new FileReader(metaData))) {
      final double[] meta = readLineDouble(br.readLine());
      final int classes = (int) meta[0];
      int CURRENT_FEATURES = (int) meta[1];
      final double[][] inputCoef = new double[classes][CURRENT_FEATURES];

      String line;
      for (int index = 0; index < classes; index++) {
        line = br.readLine();
        final double[] numbers = readLineDouble(line);

        assert numbers.length == CURRENT_FEATURES;
        inputCoef[index] = numbers;
      }

      line = br.readLine();
      intercept = readLineDouble(line);
      weights = new DoubleMatrix(inputCoef).transpose();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Topic[] predict(Document document) {
    final DoubleMatrix probs = predictProba(new DoubleMatrix(document.getTfidfRepresentation()));
    final Topic[] result = new Topic[probs.length];
    for (int index = 0; index < probs.data.length; index++) {
      result[index] = new Topic("get name from meta data", Integer.toString(index), probs.data[index]);
    }

    return result;
  }

  static double[] readLineDouble(String line) {
    return Arrays
            .stream(line.split(" "))
            .mapToDouble(Double::parseDouble)
            .toArray();
  }

  // see _predict_proba_lr in base.py sklearn
  DoubleMatrix predictProba(DoubleMatrix documents) {
    DoubleMatrix probabilities = decisionFunction(documents);

    probabilities = probabilities.mul(-1);
    probabilities = exp(probabilities);
    probabilities.add(1);
    probabilities = MatrixFunctions.powi(probabilities, -1);

    if (probabilities.rows == 1) {
        DoubleMatrix top = probabilities.mul(-1).add(1);
        return DoubleMatrix.concatVertically(top, probabilities).transpose();
    }

    final double[][] matrix = new double[1][probabilities.rows];
    matrix[0] = probabilities.rowSums().data;
    final DoubleMatrix denominator = new DoubleMatrix(matrix);
    denominator.reshape(probabilities.rows, probabilities.rows);

    return probabilities.div(denominator);
  }

  private DoubleMatrix decisionFunction(DoubleMatrix documents) {
    if (weights == null) {
      loadMeta();
    }

    final DoubleMatrix inter = new DoubleMatrix(1, intercept.length, intercept);
    final DoubleMatrix score = documents.transpose().mmul(weights);
    return score.add(inter);
  }
}
