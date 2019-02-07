package com.spbsu.flamestream.example.bl.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.RowsVecArrayMx;
import com.expleague.commons.math.vectors.impl.vectors.ArrayVec;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.google.common.annotations.VisibleForTesting;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class SklearnSgdPredictor implements TopicsPredictor {
  private final String weightsPath;
  private final String cntVectorizerPath;

  //lazy loading
  private TObjectIntMap<String> countVectorizer;
  private Vec intercept;
  private Mx weights;
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

      final Vec[] coef = new Vec[classes];
      String line;
      for (int index = 0; index < classes; index++) {
        line = br.readLine();
        final double[] numbers = parseDoubles(line);
        final ArrayVec vecNumbers = new ArrayVec(numbers, 0, numbers.length);

        assert numbers.length == currentFeatures;
        coef[index] = vecNumbers;
      }

      weights = new RowsVecArrayMx(coef);
      MxTools.transpose(weights);

      line = br.readLine();
      final double[] parsedIntercept = parseDoubles(line);
      intercept = new ArrayVec(parsedIntercept, 0, parsedIntercept.length);
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
    final SparseVec vectorized = vectorize(document);

    final Vec score = MxTools.multiply(weights, vectorized);
    final Vec sum = VecTools.sum(score, intercept);
    final Vec muli = VecTools.scale(sum, -1);
    VecTools.exp(muli);

    final double[] ones = new double[score.dim()];
    Arrays.fill(ones, 1);
    final Vec vecOnes = new ArrayVec(ones, 0, ones.length);
    final Vec sum1 = VecTools.sum(muli, vecOnes);

    // powi -1
    for (int i = 0; i < sum1.dim(); i++) {
      double changed = 1 / sum1.get(i);
      sum1.set(i, changed);
    }

    double rowSum = VecTools.sum(sum1);
    VecTools.scale(sum1, 1 / rowSum);

    final Topic[] result = new Topic[sum1.dim()];
    for (int index = 0; index < sum1.dim(); index++) {
      result[index] = new Topic(topics[index], Integer.toString(index), sum1.get(index));
    }
    return result;
  }

  @VisibleForTesting
  SparseVec vectorize(Document document) {
    loadVocabulary();
    final TObjectDoubleMap<String> tfidf = document.getTfidf();
    final int[] indeces = new int[tfidf.size()];
    final double[] values = new double[tfidf.size()];

    int index = 0;
    for (String key : tfidf.keySet()) {
      final int valueIndex = countVectorizer.get(key);

      indeces[index] = valueIndex;
      values[index] = tfidf.get(key);
      index++;
    }

    return new SparseVec(countVectorizer.size(), indeces, values);
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
