package com.spbsu.flamestream.example.bl.classifier;

import com.google.common.annotations.VisibleForTesting;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.jblas.DoubleMatrix;
import org.jblas.MatrixFunctions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static org.jblas.MatrixFunctions.expi;

public class SklearnSgdPredictor implements TopicsPredictor {
  private final String weightsPath;
  private final String cntVectorizerPath;

  //lazy loading
  private TObjectIntMap<String> countVectorizer;
  private DoubleMatrix intercept;
  private DoubleMatrix weights;
  private String[] topics;

  SklearnSgdPredictor(String cntVectorizerPath, String weightsPath) {
    this.weightsPath = weightsPath;
    this.cntVectorizerPath = cntVectorizerPath;
  }

  private void loadMeta() {
    if (weights != null) {
      return;
    }

    final File metaData = new File(weightsPath);
    try (final BufferedReader br = new BufferedReader(new FileReader(metaData))) {
      final double[] meta = parseDoubles(br.readLine());
      final int classes = (int) meta[0];
      final int currentFeatures = (int) meta[1];
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
      final double[] parsedIntercept = parseDoubles(line);
      intercept = new DoubleMatrix(1, parsedIntercept.length, parsedIntercept);
      weights = new DoubleMatrix(inputCoef).transpose();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void loadVocabulary() {
    if (countVectorizer != null) {
      return;
    }

    final File countFile = new File(cntVectorizerPath);
    countVectorizer = new TObjectIntHashMap<>();
    try (final BufferedReader countFileReader = new BufferedReader(new FileReader(countFile))) {
      String line;
      while ((line = countFileReader.readLine()) != null) {
        final String[] items = line.split(" ");
        final String key = items[0];
        final int value = Integer.parseInt(items[1]);
        countVectorizer.put(key, value);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Topic[] predict(Document document) {
    loadMeta();
    loadVocabulary();

    // TODO: 07.02.19 migrate to sparse matrices
    final double[] vectorized = vectorize(document);
    final DoubleMatrix score = new DoubleMatrix(vectorized).transpose().mmul(weights);
    score.addi(intercept);
    score.muli(-1);
    expi(score);
    score.addi(1);
    MatrixFunctions.powi(score, -1);
    score.divi(score.rowSums());

    final Topic[] result = new Topic[score.length];
    for (int index = 0; index < score.data.length; index++) {
      result[index] = new Topic(topics[index], Integer.toString(index), score.data[index]);
    }
    return result;
  }

  @VisibleForTesting
  double[] vectorize(Document document) {
    loadVocabulary();
    final double[] res = new double[countVectorizer.size()];
    final TObjectDoubleMap<String> tfidf = document.getTfidf();
    tfidf.forEachEntry((s, v) -> {
      res[countVectorizer.get(s)] = v;
      return true;
    });
    return res;
  }

  @VisibleForTesting
  int wordIndex(String word) {
    loadVocabulary();
    return countVectorizer.get(word);
  }

  @VisibleForTesting
  static double[] parseDoubles(String line) {
    return Arrays
            .stream(line.split(" "))
            .mapToDouble(Double::parseDouble)
            .toArray();
  }
}
