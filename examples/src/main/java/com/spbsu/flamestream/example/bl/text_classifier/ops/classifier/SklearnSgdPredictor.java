package com.spbsu.flamestream.example.bl.text_classifier.ops.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.RowsVecArrayMx;
import com.expleague.commons.math.vectors.impl.vectors.ArrayVec;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class SklearnSgdPredictor implements TopicsPredictor {
  private final String weightsPath;

  //lazy loading
  private Vec intercept;
  private Mx weights;
  private String[] topics;

  // TODO: 13.05.19 remove me
  public String[] getTopics() {
    return topics;
  }

  public SklearnSgdPredictor(String weightsPath) {
    this.weightsPath = weightsPath;
  }

  @Override
  public Topic[] predict(Vec vec) {
    init();
    final Vec probabilities;
    { // compute topic probabilities
      final Vec score = MxTools.multiply(weights, vec);
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

  public void init() {
    if (weights != null) {
      return;
    }

    final File metaData = new File(weightsPath);
    try (final BufferedReader br = new BufferedReader(new InputStreamReader(
            new FileInputStream(metaData),
            StandardCharsets.UTF_8
    ))) {
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

  private static double[] parseDoubles(String line) {
    return Arrays
            .stream(line.split(" "))
            .mapToDouble(Double::parseDouble)
            .toArray();
  }
}
