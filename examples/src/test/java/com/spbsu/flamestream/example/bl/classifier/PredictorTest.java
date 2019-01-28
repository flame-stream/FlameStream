package com.spbsu.flamestream.example.bl.classifier;

import org.testng.annotations.Test;

import java.io.*;

import static com.spbsu.flamestream.example.bl.classifier.Predictor.parseDoubles;
import static java.lang.Math.abs;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PredictorTest {
  private static File testDataFile = new File("src/test/resources/sklearn_prediction");
  private static File idfFile = new File("src/test/resources/idf_matrix");
  private double[] idf;

  private void loadIdf() throws IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(idfFile))) {
      idf = parseDoubles(br.readLine());
    }
  }

  @Test
  public void fiveDocumentTest() throws IOException {
    loadIdf();

    final Predictor predictor = new Predictor();
    try (BufferedReader br = new BufferedReader(new FileReader(testDataFile))) {
      final int testCount = Integer.parseInt(br.readLine());
      // five python predictions provided by script
      for (int i = 0; i < testCount; i++) {
        final Document document = new Document(null); // replace with map: create tfidf
        final double[] pyPrediction = parseDoubles(br.readLine());
        final Topic[] prediction = predictor.predict(document);

        assertEquals(prediction.length, pyPrediction.length);
        for (int j = 0; j < prediction.length; j++) {
          double diff = abs(pyPrediction[j] - prediction[j].getProbability());
          assertTrue(diff < 2e-2);
        }
      }
    }
  }
}
