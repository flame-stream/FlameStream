package com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier;

import com.expleague.commons.math.MathTools;
import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecIterator;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.expleague.commons.math.vectors.impl.vectors.ArrayVec;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class FTRLProximalOptimizer implements Optimizer, BiClassifierOptimizer {

  public static class Builder {
    private double alpha = 0.2;
    private double beta = 0.1;
    private double lambda1 = 0.1;
    private double lambda2 = 0.001;

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

    public FTRLProximalOptimizer build() {
      return new FTRLProximalOptimizer(alpha, beta, lambda1, lambda2);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FTRLProximalOptimizer.class.getName());

  private final double alpha;
  private final double beta;
  private final double lambda1;
  private final double lambda2;

  private FTRLProximalOptimizer(double alpha, double beta, double lambda1, double lambda2) {
    this.alpha = alpha;
    this.beta = beta;
    this.lambda1 = lambda1;
    this.lambda2 = lambda2;
  }

  @Override
  public Vec optimizeWeights(List<DataPoint> trainingSet, int[] isCorrect, Vec prevWeights) {
    final int dim = trainingSet.get(0).getFeatures().dim();
    final Vec zed = new ArrayVec(dim);
    final Vec norm = new ArrayVec(dim);
    final SparseVec w = new SparseVec(prevWeights.dim());

    for (int t = 0; t < trainingSet.size(); t++) {
      final Vec x = trainingSet.get(t).getFeatures();
      final VecIterator iterator = x.nonZeroes();
      while (iterator.advance()) {
        final int index = iterator.index();
        final double z = zed.get(index);
        if (Math.abs(z) > lambda1) {
          double val = -(z - Math.signum(z) * lambda1) /
                  ((beta + Math.sqrt(norm.get(index))) / alpha + lambda2);
          w.set(index, val);
        }
      }
      final double p = MathTools.sigmoid(VecTools.multiply(x, w));

      iterator.seek(0);
      while (iterator.advance()) {
        final int index = iterator.index();
        final double g = (p - isCorrect[t]) * iterator.value();
        final double sigma = (Math.sqrt(norm.get(index) + g * g) - Math.sqrt(norm.get(index))) / alpha;
        zed.set(index, zed.get(index) + g - sigma * w.get(index));
        norm.set(index, norm.get(index) + g * g);
      }
    }

    return w;
  }

  /*@Override
  public Mx optimizeWeights(List<DataPoint> trainingSet, Mx prevWeights, String[] topics) {
    List<String> topicList = Arrays.asList(topics);

    final int[] indices = trainingSet.stream().mapToInt(s -> topicList.indexOf(s.getLabel())).toArray();
    final Mx weights = new SparseMx(prevWeights.rows(), prevWeights.columns());
    final Mx zed = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    final Mx norm = new VecBasedMx(prevWeights.rows(), prevWeights.columns());

    for (int i = 0; i < trainingSet.size(); i++) {
      final int finalI = i;
      Vec x = trainingSet.get(i).getFeatures();
      IntStream.range(0, weights.rows()).parallel().forEach(j -> {
        VecIterator iterator = x.nonZeroes();
        while (iterator.advance()) {
          double z = zed.get(j, iterator.index());
          if (Math.abs(z) > lambda1) {
            double val = -(z - Math.signum(z) * lambda1) /
                    ((beta + Math.sqrt(norm.get(j, iterator.index()))) / alpha + lambda2);
            weights.set(j, iterator.index(), val);
          } else {
            weights.set(j, iterator.index(), 0);
          }
        }
      });
      Vec p = MxTools.multiply(weights, x);
      VecTools.exp(p);
      double denom = VecTools.sum(p);
      VecTools.scale(p, 1 / denom);

      IntStream.range(0, weights.rows()).parallel().forEach(j -> {
        VecIterator iterator = x.nonZeroes();
        while (iterator.advance()) {
          double g = (indices[finalI] == j ? p.get(j) - 1 : p.get(j)) * iterator.value();
          double sigma =
                  (Math.sqrt(norm.get(j, iterator.index()) + g * g) - Math.sqrt(norm.get(j, iterator.index()))) / alpha;
          zed.set(j, iterator.index(), zed.get(j, iterator.index()) + g - sigma * weights.get(j, iterator.index()));
          norm.set(j, iterator.index(), norm.get(j, iterator.index()) + g * g);
        }
      });
    }

    Mx ans = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    for (int i = 0; i < weights.rows(); i++) {
      VecIterator nz = weights.row(i).nonZeroes();
      while (nz.advance()) {
        ans.set(i, nz.index(), nz.value());
      }
    }
    return ans;
  }*/

  public Mx optimizeWeights(List<DataPoint> trainingSet, Mx prevWeights, String[] topics) {
    State state = new State(prevWeights);

    for (DataPoint aTrainingSet : trainingSet) {
      state = optimizeState(aTrainingSet, state, topics);
    }

    Mx ans = new VecBasedMx(prevWeights.rows(), prevWeights.columns());
    for (int i = 0; i < prevWeights.rows(); i++) {
      VecIterator nz = state.weights.row(i).nonZeroes();
      while (nz.advance()) {
        ans.set(i, nz.index(), nz.value());
      }
    }
    return ans;
  }

  @Override
  public State optimizeState(DataPoint trainingPoint, State prevState, String[] topics) {
    List<String> topicList = Arrays.asList(topics);
    final int index = topicList.indexOf(trainingPoint.getLabel());
    final Vec x = trainingPoint.getFeatures();
    IntStream.range(0, prevState.weights.rows()).parallel().forEach(j -> {
      VecIterator iterator = x.nonZeroes();
      while (iterator.advance()) {
        double z = prevState.zed.get(j, iterator.index());
        if (Math.abs(z) > lambda1) {
          double val = -(z - Math.signum(z) * lambda1) /
                  ((beta + Math.sqrt(prevState.norm.get(j, iterator.index()))) / alpha + lambda2);
          prevState.weights.set(j, iterator.index(), val);
        } else {
          prevState.weights.set(j, iterator.index(), 0);
        }
      }
    });
    Vec p = MxTools.multiply(prevState.weights, x);
    VecTools.exp(p);
    double denom = VecTools.sum(p);
    VecTools.scale(p, 1 / denom);

    IntStream.range(0, prevState.weights.rows()).parallel().forEach(j -> {
      VecIterator iterator = x.nonZeroes();
      while (iterator.advance()) {
        double g = (index == j ? p.get(j) - 1 : p.get(j)) * iterator.value();
        double sigma =
                (Math.sqrt(prevState.norm.get(j, iterator.index()) + g * g) - Math.sqrt(prevState.norm.get(j, iterator.index()))) / alpha;
        prevState.zed.set(j, iterator.index(), prevState.zed.get(j, iterator.index()) + g - sigma * prevState.weights.get(j, iterator.index()));
        prevState.norm.set(j, iterator.index(), prevState.norm.get(j, iterator.index()) + g * g);
      }
    });
    return prevState;
  }
}
