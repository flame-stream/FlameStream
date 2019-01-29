package com.spbsu.flamestream.example.bl.classifier;

import org.testng.annotations.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static com.spbsu.flamestream.example.bl.classifier.Predictor.parseDoubles;
import static java.lang.Math.abs;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PredictorTest {
  private static File testDataFile = new File("src/test/resources/sklearn_prediction");

  @Test
  public void threeDocumentTest() throws IOException {
    final Predictor predictor = new Predictor();
    try (BufferedReader br = new BufferedReader(new FileReader(testDataFile))) {
      final double[] data = parseDoubles(br.readLine());
      final int testCount = (int) data[0];
      final int features = (int) data[1];

      // five python predictions provided by script
      for (int i = 0; i < testCount; i++) {
        final double[] pyPrediction = parseDoubles(br.readLine());
        final String[] doc = br.readLine().split(" ");
        final double[] tfidfFeatures = parseDoubles(br.readLine());

        final Map<String, Double> tfidf = new HashMap<>();
        for (int j = 0; j < doc.length; j++) {
          tfidf.put(doc[j], tfidfFeatures[j]);
        }

        final Document document = new Document(tfidf);
        final Topic[] prediction = predictor.predict(document);

        assertEquals(prediction.length, pyPrediction.length);
        for (int j = 0; j < prediction.length; j++) {
          final double diff = abs(pyPrediction[j] - prediction[j].getProbability());
          assertTrue(diff < 0.035);
        }
      }
    }
  }
}
