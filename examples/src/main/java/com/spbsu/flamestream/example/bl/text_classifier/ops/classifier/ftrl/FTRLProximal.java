package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ftrl;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecIterator;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.vectors.ArrayVec;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.DataPoint;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.OnlineModel;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.Topic;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class FTRLProximal implements OnlineModel {

  public static class Builder {
    private double alpha = 132;
    private double beta = 0.1;
    private double lambda1 = 0.0086;
    private double lambda2 = 0.095;

    public Builder alpha(double alpha) {
      this.alpha = alpha;
      return this;
    }

    public Builder beta(double beta) {
      this.beta = beta;
      return this;
    }

    public Builder lambda1(double lambda1) {
      this.lambda1 = lambda1;
      return this;
    }

    public Builder lambda2(double lambda2) {
      this.lambda2 = lambda2;
      return this;
    }

    public FTRLProximal build(String[] allTopics) {
      return new FTRLProximal(alpha, beta, lambda1, lambda2, allTopics);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private final double alpha;
  private final double beta;
  private final double lambda1;
  private final double lambda2;
  private final String[] topics;

  private FTRLProximal(double alpha, double beta, double lambda1, double lambda2, final String[] topics) {
    this.alpha = alpha;
    this.beta = beta;
    this.lambda1 = lambda1;
    this.lambda2 = lambda2;
    this.topics = topics;
  }

  @Override
  public ModelState step(DataPoint trainingPoint, ModelState prevState) {
    List<String> topicList = Arrays.asList(topics);
    final int index = topicList.indexOf(trainingPoint.getLabel());
    final Vec x = trainingPoint.getFeatures();
    final Mx weights = prevState.weights();
    final Mx zed = ((FTRLState) prevState).zed();
    final Mx norm = ((FTRLState) prevState).norm();
    IntStream.range(0, weights.rows()).parallel().forEach(j -> {
      final VecIterator iterator = x.nonZeroes();
      while (iterator.advance()) {
        final int xindex = iterator.index();
        final double z = zed.get(j, xindex);
        if (Math.abs(z) > lambda1) {
          double val = -(z - Math.signum(z) * lambda1) /
                  ((beta + Math.sqrt(norm.get(j, xindex))) / alpha + lambda2);
          weights.set(j, xindex, val);
        } else {
          weights.set(j, xindex, 0);
        }
      }
    });

    final double[] p = new double[weights.rows()];
    double denom = 0;
    for (int j = 0; j < weights.rows(); j++) {
      VecIterator xNz = x.nonZeroes();
      p[j] = 0;
      while (xNz.advance()) {
        p[j] += xNz.value() * weights.get(j, xNz.index());
      }
      p[j] = Math.exp(p[j]);
      denom += p[j];
    }
    for (int j = 0; j < weights.rows(); j++) {
      p[j] /= denom;
    }

    IntStream.range(0, weights.rows()).parallel().forEach(j -> {
      final VecIterator iterator = x.nonZeroes();
      while (iterator.advance()) {
        final int xindex = iterator.index();
        final double w = weights.get(j, xindex);
        double g = (index == j ? p[j] - 1 : p[j]) * iterator.value();
        double sigma =
                (Math.sqrt(norm.get(j, xindex) + g * g) - Math.sqrt(norm.get(j, xindex))) / alpha;
        zed.set(j, xindex, zed.get(j, xindex) + g - sigma * w);
        norm.set(j, xindex, norm.get(j, xindex) + g * g);
      }
    });
    return prevState;
  }

  @Override
  public Topic[] predict(ModelState state, Vec vec) {
    final Vec probabilities;
    { // compute topic probabilities
      final Vec score = MxTools.multiply(state.weights(), vec);
      final Vec scaled = VecTools.scale(score, -1);
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

  @Override
  public int classes() {
    return topics.length;
  }
}
