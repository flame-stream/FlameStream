package com.spbsu.flamestream.example.bl.classifier;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static com.spbsu.flamestream.example.bl.classifier.SklearnSgdPredictor.parseDoubles;
import static java.lang.Math.abs;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PredictorTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PredictorTest.class.getName());
  private static File testDataFile = new File("src/test/resources/sklearn_prediction");

  @Test
  public void threeDocumentTest() throws IOException {
    final String cntVectorizerPath = "src/main/resources/cnt_vectorizer";
    final String weightsPath = "src/main/resources/classifier_weights";

    final SklearnSgdPredictor predictor = new SklearnSgdPredictor(cntVectorizerPath, weightsPath);
    try (BufferedReader br = new BufferedReader(new FileReader(testDataFile))) {
      final double[] data = parseDoubles(br.readLine());
      final int testCount = (int) data[0];

      // five python predictions provided by script
      for (int i = 0; i < testCount; i++) {
        final double[] pyPrediction = parseDoubles(br.readLine());
        final String[] doc = br.readLine().split(" ");
        final double[] tfidfFeatures = parseDoubles(br.readLine());

        final TObjectDoubleMap<String> tfidf = new TObjectDoubleHashMap<>();
        for (int j = 0; j < doc.length; j++) {
          tfidf.put(doc[j], tfidfFeatures[j]);
        }

        final Document document = new Document(tfidf);
        final Topic[] prediction = predictor.predict(document);

        assertEquals(prediction.length, pyPrediction.length);
        for (int j = 0; j < prediction.length; j++) {
          final double diff = abs(pyPrediction[j] - prediction[j].probability());
          assertTrue(diff < 0.035);
        }

        Arrays.sort(prediction);
        LOGGER.info("Doc: {}", (Object) doc);
        LOGGER.info("Predict: {}", (Object) prediction);
      }
    }
  }
}
