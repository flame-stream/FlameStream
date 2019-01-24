package com.spbsu.flamestream.example.bl.classifier;

import org.jblas.DoubleMatrix;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static com.spbsu.flamestream.example.bl.classifier.Predictor.readLineDouble;
import static java.lang.Math.abs;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PredictorTest {
  private static File testData = new File("src/test/resources/test_data");

  @Test
  public void fiveDocumentTest() throws IOException {
    final Predictor predictor = new Predictor();
    try (BufferedReader br = new BufferedReader(new FileReader(testData))) {
      final int testCount = Integer.parseInt(br.readLine());
      // five python predictions provided by script
      for (int i = 0; i < testCount; i++) {
        final double[] document = readLineDouble(br.readLine());
        final double[] pyPrediction = readLineDouble(br.readLine());
        final DoubleMatrix prediction = predictor.predictProba(document);

        assertEquals(prediction.length, pyPrediction.length);
        for (int j = 0; j < prediction.length; j++) {
          double diff = abs(pyPrediction[j] - prediction.get(0, j));
          assertTrue(diff > 5e-3);
        }
      }
    }
  }
}
