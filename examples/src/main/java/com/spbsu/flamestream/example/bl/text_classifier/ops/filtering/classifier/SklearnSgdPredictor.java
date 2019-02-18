package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.RowsVecArrayMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SklearnSgdPredictor implements TopicsPredictor {
  private static final Pattern PATTERN = Pattern.compile("\\b\\w\\w+\\b", Pattern.UNICODE_CHARACTER_CLASS);

  private final String weightsPath;
  private final String cntVectorizerPath;

  //lazy loading
  private TObjectIntMap<String> countVectorizer;
  private Vec intercept;
  private Mx weights;
  private Mx prevWeights;
  private String[] topics;

  public SklearnSgdPredictor(String cntVectorizerPath, String weightsPath) {
    this.weightsPath = weightsPath;
    this.cntVectorizerPath = cntVectorizerPath;
  }

  @Override
  public Topic[] predict(Document document) {
    loadMeta();
    loadVocabulary();

    final Map<String, Double> tfIdf = document.tfIdf();
    final int[] indices = new int[tfIdf.size()];
    final double[] values = new double[tfIdf.size()];
    { //convert TF-IDF features to sparse vector
      int ind = 0;
      for (String key : tfIdf.keySet()) {
        final int valueIndex = countVectorizer.get(key);
        indices[ind] = valueIndex;
        values[ind] = tfIdf.get(key);
        ind++;
      }
    }

    final Vec probabilities;
    { // compute topic probabilities
      final SparseVec vectorized = new SparseVec(countVectorizer.size(), indices, values);
      final Vec score = MxTools.multiply(weights, vectorized);
      final Vec sum = VecTools.sum(score, intercept);
      final Vec scaled = VecTools.scale(sum, -1);
      VecTools.exp(scaled);

      final double[] ones = new double[score.dim()];
      Arrays.fill(ones, 1);
      final Vec vecOnes = new ArrayVec(ones, 0, ones.length);
      probabilities = VecTools.sum(scaled, vecOnes);
      for (int i = 0; i < probabilities.dim(); i++) {
        double changed = 1 / probabilities.get(i);
        probabilities.set(i, changed);
      }
      final double rowSum = VecTools.sum(probabilities);
      VecTools.scale(probabilities, 1 / rowSum);
    }

    final Topic[] result = new Topic[probabilities.dim()];
    { //fill in topics
      for (int index = 0; index < probabilities.dim(); index++) {
        result[index] = new Topic(topics[index], Integer.toString(index), probabilities.get(index));
      }
    }
    return result;
  }

  private double softmaxValue(Vec xi, int correctTopicIndex) {
    final double numer = Math.exp(VecTools.multiply(weights.row(correctTopicIndex), xi));
    double denom = 0.0;
    for (int k = 0; k < weights.rows(); k++) {
      denom += Math.exp(VecTools.multiply(weights.row(k), xi));
    }

    return numer / denom;
  }

  // https://stats.stackexchange.com/questions/265905/derivative-of-softmax-with-respect-to-weights
  private Mx softmaxGradient(Mx trainingSet, String[] correctTopics) {
    List<String> topicList = Arrays.asList(topics);

    Vec[] gradients = new Vec[weights.rows()];
    for (int i = 0; i < weights.rows(); i++) {
      final Vec wi = new ArrayVec(weights.row(i).toArray(), 0, weights.columns());

      for (int j = 0; j < trainingSet.rows(); j++) {
        final Vec x = trainingSet.row(j);
        final int index = topicList.indexOf(correctTopics[j]);
        final double value1 = softmaxValue(x, index);

        double croneker = 1.0;
        if (i != j) {
          croneker = 0.0;
        }

        final double value2 = softmaxValue(x, i);
        VecTools.scale(x, value1 * (croneker - value2));

        VecTools.subtract(wi, x); // gradient subsctract
      }

      gradients[i] = wi;
    }

    return new RowsVecArrayMx(gradients);
  }

  // https://jamesmccaffrey.wordpress.com/2017/06/27/implementing-neural-network-l1-regularization/
  // https://visualstudiomagazine.com/articles/2017/12/05/neural-network-regularization.aspx
  private Mx l1Gradient(double lambda) {
    Mx gradient = new VecBasedMx(weights.rows(), weights.columns());

    for (int i = 0; i < weights.rows(); i++) {
      for (int j = 0; j < weights.columns(); j++) {
        gradient.set(i, j, lambda * Math.signum(weights.get(i, j)));
      }
    }

    return gradient;
  }

  // https://jamesmccaffrey.wordpress.com/2017/02/19/l2-regularization-and-back-propagation/
  // https://visualstudiomagazine.com/articles/2017/09/01/neural-network-l2.aspx
  private Mx l2Gradient(double lambda) {
    Mx gradient = new VecBasedMx(weights.rows(), weights.columns());

    for (int i = 0; i < weights.rows(); i++) {
      for (int j = 0; j < weights.columns(); j++) {
        gradient.set(i, j, lambda * 2 * (weights.get(i, j) - prevWeights.get(i, j)));
      }
    }

    return gradient;
  }

  @Override
  public void updateWeights(Mx trainingSet, String[] correctTopics) {
    final double lambda1 = 0.001;
    final double lambda2 = 0.001;

    final Mx softmax = softmaxGradient(trainingSet, correctTopics);
    final Mx l1 = l1Gradient(lambda1);
    final Mx l2 = l2Gradient(lambda2);

    prevWeights = weights;

    Mx updated = new VecBasedMx(weights.rows(), weights.columns());
    for (int i = 0; i < weights.rows(); i++) {
      for (int j = 0; j < weights.columns(); j++) {
        final double value = weights.get(i, j) - softmax.get(i, j) - l1.get(i, j) - l2.get(i, j);
        updated.set(i, j, value);
      }
    }

    weights = updated;
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
      prevWeights = new RowsVecArrayMx(coef);
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

  @VisibleForTesting
  public int wordIndex(String word) {
    loadVocabulary();
    return countVectorizer.get(word);
  }

  public static Stream<String> text2words(String text) {
    final Matcher matcher = PATTERN.matcher(text);
    final Iterable<String> iterable = () -> new Iterator<String>() {
      @Override
      public boolean hasNext() {
        return matcher.find();
      }

      @Override
      public String next() {
        return matcher.group(0);
      }
    };
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  private static double[] parseDoubles(String line) {
    return Arrays
            .stream(line.split(" "))
            .mapToDouble(Double::parseDouble)
            .toArray();
  }
}
