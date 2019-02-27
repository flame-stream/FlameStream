package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class SoftmaxRegressionOptimizer implements Optimizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SoftmaxRegressionOptimizer.class.getName());
  private final List<String> topicList;

  public SoftmaxRegressionOptimizer(String[] topics) {
    topicList = Arrays.asList(topics);
  }

  private Vec computeSoftmaxValues(SparseMx weights, Mx trainingSet, int[] correctTopics) {
    Vec softmaxValues = new SparseVec(trainingSet.rows());

    for (int i = 0; i < trainingSet.rows(); i++) {
      final Vec x = trainingSet.row(i);
      final int index = correctTopics[i];
      final Vec mul = MxTools.multiply(weights, x);
      VecTools.exp(mul);

      final double numer = mul.get(index);
      double denom = 0.0;
      for (int k = 0; k < weights.rows(); k++) {
        denom += mul.get(k);
      }

      softmaxValues.set(i, numer / denom);
    }

    return softmaxValues;
  }

  private double softmaxGradient(SparseMx weights, Mx result, Mx trainingSet, int[] correctTopics) {
    final SparseVec[] gradients = new SparseVec[weights.rows()];
    final Vec softmaxValues = computeSoftmaxValues(weights, trainingSet, correctTopics);

    for (int j = 0; j < weights.rows(); j++) {
      //LOGGER.info("weights {} component", j);
      SparseVec grad = new SparseVec(weights.columns());
      final SparseVec scales = new SparseVec(trainingSet.rows());
      for (int i = 0; i < trainingSet.rows(); i++) {
        final int index = correctTopics[i];
        final int indicator = index == j ? 1 : 0;
        scales.set(i, indicator - softmaxValues.get(i));
      }

      for (int i = 0; i < trainingSet.rows(); i++) {
        final Vec x = trainingSet.row(i);
        VecTools.scale(x, scales);
        grad = VecTools.sum(grad, x);
      }

      gradients[j] = VecTools.scale(grad, -1.0 / trainingSet.rows());
    }

    result = new SparseMx(gradients);
    return VecTools.sum(softmaxValues);
  }

  private Mx l1Gradient(SparseMx weights) {
    final Mx gradient = new SparseMx(weights.rows(), weights.columns());

    for (int i = 0; i < weights.rows(); i++) {
      for (int j = 0; j < weights.columns(); j++) {
        gradient.set(i, j, Math.signum(weights.get(i, j)));
      }
    }

    return gradient;
  }

  private Mx l2Gradient(SparseMx weights, SparseMx prevWeights) {
    //return VecTools.subtract(VecTools.scale(weights, 2), prevWeights); ???
    Mx gradient = new SparseMx(weights.rows(), weights.columns());

    for (int i = 0; i < weights.rows(); i++) {
      for (int j = 0; j < weights.columns(); j++) {
        gradient.set(i, j, 2 * (weights.get(i, j) - prevWeights.get(i, j)));
      }
    }

    return gradient;
  }

  public SparseMx optimizeWeights(Mx trainingSet, String[] correctTopics, SparseMx weights) {
    final double alpha = 1e-3;
    final double lambda1 = 0.0000009; // same as in python script
    //final double lambda2 = 1e-3;
    final double maxIter = 100;
    final int[] indeces = Stream.of(correctTopics).mapToInt(topicList::indexOf).toArray();

    double previousValue = 0;
    SparseMx prevWeights = weights;
    for (int iteration = 1; iteration <= maxIter; iteration++) {
      LOGGER.info("Iteration {}", iteration);
      Mx softmax = new SparseMx(weights.rows(), weights.columns());
      double softmaxValue = softmaxGradient(weights, softmax, trainingSet, indeces);
      LOGGER.info("Softmax value : {}", softmaxValue);
      if (Math.abs(softmaxValue - previousValue) < 1e-3) {
        break;
      }

      previousValue = softmaxValue;
      Mx l1 = l1Gradient(weights);
      //Mx l2 = l2Gradient();

      prevWeights = weights;
      //softmax = VecTools.scale(softmax, alpha);
      //l1 = VecTools.scale(l1, lambda1);
      //l2 = VecTools.scale(l2, lambda2);
      // weights = VecTools.subtract(weights, VecTools.sum(softmax, VecTools.sum(l1, l2)));

      SparseMx updated = new SparseMx(weights.rows(), weights.columns());
      for (int i = 0; i < weights.rows(); i++) {
        for (int j = 0; j < weights.columns(); j++) {
          final double value = weights.get(i, j) - alpha * softmax.get(i, j) - lambda1 * l1.get(i, j);
          updated.set(i, j, value);
        }
      }

      weights = updated;
    }

    return weights;
  }

}
