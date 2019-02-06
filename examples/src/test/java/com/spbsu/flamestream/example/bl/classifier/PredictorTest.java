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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.spbsu.flamestream.example.bl.classifier.SklearnSgdPredictor.parseDoubles;
import static java.lang.Math.abs;
import static java.lang.Math.max;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PredictorTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PredictorTest.class.getName());
  private static File testDataFile = new File("src/test/resources/sklearn_prediction");

  @Test
  public void sklearnDocumentTest() throws IOException {
    final String cntVectorizerPath = "src/main/resources/cnt_vectorizer";
    final String weightsPath = "src/main/resources/classifier_weights";

    final SklearnSgdPredictor predictor = new SklearnSgdPredictor(cntVectorizerPath, weightsPath);
    try (BufferedReader br = new BufferedReader(new FileReader(testDataFile))) {
      final double[] data = parseDoubles(br.readLine());
      final int testCount = (int) data[0];

      for (int i = 0; i < testCount; i++) {
        final double[] pyPrediction = parseDoubles(br.readLine());
        final String doc = br.readLine().toLowerCase();
        final double[] tfidfFeatures = parseDoubles(br.readLine());

        final Pattern pattern = Pattern.compile("\\b\\w\\w+\\b", Pattern.UNICODE_CHARACTER_CLASS);
        final Matcher matcher = pattern.matcher(doc);
        final TObjectDoubleMap<String> tfidf = new TObjectDoubleHashMap<>();
        while (matcher.find()) {
          final String word = matcher.group(0);
          final int featureIndex = predictor.vectorize(word);
          tfidf.put(word, tfidfFeatures[featureIndex]);
        }

        final Document document = new Document(tfidf);
        final double[] myVectorization = predictor.vectorize(document);
        double maxDiff = 0;
        for (int j = 0; j < myVectorization.length; j++) {
          final double diff = abs(tfidfFeatures[j] - myVectorization[j]);
          maxDiff = max(diff, maxDiff);
        }
        final Topic[] prediction = predictor.predict(document);

        assertEquals(prediction.length, pyPrediction.length);
        maxDiff = 0;
        for (int j = 0; j < prediction.length; j++) {
          final double diff = abs(pyPrediction[j] - prediction[j].probability());
          maxDiff = max(diff, maxDiff);
        }

        assertTrue(maxDiff < 0.15);
        Arrays.sort(prediction);
        LOGGER.info("Doc: {}", doc);
        LOGGER.info("Max diff {} in predictions", maxDiff);
        LOGGER.info("Predict: {}", (Object) prediction);
        LOGGER.info("\n");
      }
    }
  }
}
