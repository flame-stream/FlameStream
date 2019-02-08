package com.spbsu.flamestream.example.bl.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.RowsVecArrayMx;
import com.expleague.commons.math.vectors.impl.vectors.ArrayVec;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.google.common.annotations.VisibleForTesting;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

public class SklearnSgdPredictor implements TopicsPredictor {
  private static final Pattern PATTERN = Pattern.compile("\\b\\w\\w+\\b", Pattern.UNICODE_CHARACTER_CLASS);

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

  @Override
  public Topic[] predict(Document document) {
    loadMeta();
    loadVocabulary();

    final SparseVec vectorized = vectorize(document);
    final Vec score = MxTools.multiply(weights, vectorized);
    final Vec sum = VecTools.sum(score, intercept);
    final Vec scaled = VecTools.scale(sum, -1);
    VecTools.exp(scaled);

    final double[] ones = new double[score.dim()];
    Arrays.fill(ones, 1);
    final Vec vecOnes = new ArrayVec(ones, 0, ones.length);
    final Vec plusOne = VecTools.sum(scaled, vecOnes);

    for (int i = 0; i < plusOne.dim(); i++) {
      double changed = 1 / plusOne.get(i);
      plusOne.set(i, changed);
    }

    double rowSum = VecTools.sum(plusOne);
    VecTools.scale(plusOne, 1 / rowSum);

    final Topic[] result = new Topic[plusOne.dim()];
    for (int index = 0; index < plusOne.dim(); index++) {
      result[index] = new Topic(topics[index], Integer.toString(index), plusOne.get(index));
    }
    return result;
  }

  public void init() {
    loadMeta();
    loadVocabulary();
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
        String[] rawSplit = line.split(" ");

        int[] indeces = new int[rawSplit.length / 2];
        double[] values = new double[rawSplit.length / 2];
        for (int i = 0; i < rawSplit.length; i += 2) {
          int valueIndex = Integer.parseInt(rawSplit[i]);
          double value = Double.parseDouble(rawSplit[i + 1]);

          indeces[i / 2] = valueIndex;
          values[i / 2] = value;
        }

        final SparseVec sparseVec = new SparseVec(currentFeatures, indeces, values);

        coef[index] = sparseVec;
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

  private SparseVec vectorize(Document document) {
    loadVocabulary();
    final Map<String, Double> tfidf = document.tfIdf();
    final int[] indices = new int[tfidf.size()];
    final double[] values = new double[tfidf.size()];

    int index = 0;
    for (String key : tfidf.keySet()) {
      final int valueIndex = countVectorizer.get(key);
      indices[index] = valueIndex;
      values[index] = tfidf.get(key);
      index++;
    }
    return new SparseVec(countVectorizer.size(), indices, values);
  }

  public static Pattern pattern() {
    return PATTERN;
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
