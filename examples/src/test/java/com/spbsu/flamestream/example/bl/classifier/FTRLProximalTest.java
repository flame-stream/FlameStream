package com.spbsu.flamestream.example.bl.classifier;

import com.expleague.commons.math.MathTools;
import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.BiClassifierOptimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.FTRLProximalOptimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Optimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class FTRLProximalTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FTRLProximalTest.class.getName());
  private static final String CNT_VECTORIZER_PATH = "src/main/resources/cnt_vectorizer";
  private static final String WEIGHTS_PATH = "src/main/resources/classifier_weights";
  private static final String TMP_TRAIN_PATH = "src/test/resources/tmp_train";
  private static final String TMP_TEST_PATH = "src/test/resources/tmp_test";
  private static int testSize;
  private static int trainSize;
  private static int features;

  private final SklearnSgdPredictor predictor = new SklearnSgdPredictor(CNT_VECTORIZER_PATH, WEIGHTS_PATH);
  private final List<String> testTopics = new ArrayList<>();
  private final List<SparseVec> trainingSetList = new ArrayList<>();
  private final List<SparseVec> testSetList = new ArrayList<>();
  private String[] correctTopics;
  private String[] allTopics;

  @BeforeClass
  public void beforeClass() {
    testSize = 5000;
    trainSize = 10000;
    predictor.init();
    allTopics = Arrays.stream(predictor.getTopics()).map(String::trim).map(String::toLowerCase).toArray(String[]::new);
  }

  @AfterClass
  public void afterClass() {
    new File(TMP_TEST_PATH).delete();
    new File(TMP_TRAIN_PATH).delete();
  }

  private void splitDatset(int seed) {
    try {
      String[] commands = new String[]{
              "bash",
              "-c",
              String.format("source ~/.bashrc; cd src/test/resources/; python sklearn_split_dataset.py %d %d %d", trainSize, testSize, seed)
      };
      Process proc = Runtime.getRuntime().exec(commands);
      proc.waitFor();
      Scanner reader = new Scanner(new InputStreamReader(proc.getErrorStream()));
      while (reader.hasNextLine()) {
        LOGGER.error(reader.nextLine());
      }
      reader = new Scanner(new InputStreamReader(proc.getInputStream()));
      while (reader.hasNextLine()) {
        LOGGER.info(reader.nextLine());
      }
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void readTestTrain() {

    trainingSetList.clear();
    testSetList.clear();
    testTopics.clear();
    try {
      final List<String> trainingTopics = new ArrayList<>();
      final String[] trainInfo = Files.lines(new File(TMP_TRAIN_PATH).toPath())
              .limit(1).toArray(String[]::new)[0].split(" ");
      trainSize = Integer.parseInt(trainInfo[0]);
      features = Integer.parseInt(trainInfo[1]);


      Files.lines(new File(TMP_TRAIN_PATH).toPath())
              .skip(1)
              .forEach(line -> {
                final String[] tokens = line.split(",");
                trainingTopics.add(tokens[0].trim().toLowerCase());
                final double[] info = Arrays
                        .stream(tokens)
                        .skip(1)
                        .mapToDouble(Double::parseDouble)
                        .toArray();
                final int[] indeces = new int[info.length / 2];
                final double[] values = new double[info.length / 2];
                for (int k = 0; k < info.length; k += 2) {
                  final int index = (int) info[k];
                  final double value = info[k + 1];

                  indeces[k / 2] = index;
                  values[k / 2] = value;
                }
                trainingSetList.add(new SparseVec(features, indeces, values));
              });

      final String[] testInfo = Files.lines(new File(TMP_TRAIN_PATH).toPath())
              .limit(1).toArray(String[]::new)[0].split(" ");
      testSize = Integer.parseInt(testInfo[0]);
      features = Integer.parseInt(testInfo[1]);
      Files.lines(new File(TMP_TEST_PATH).toPath())
              .skip(1)
              .forEach(line -> {
                final String[] tokens = line.split(",");
                testTopics.add(tokens[0].trim().toLowerCase());
                final double[] info = Arrays
                        .stream(tokens)
                        .skip(1)
                        .mapToDouble(Double::parseDouble)
                        .toArray();
                final int[] indeces = new int[info.length / 2];
                final double[] values = new double[info.length / 2];
                for (int k = 0; k < info.length; k += 2) {
                  final int index = (int) info[k];
                  final double value = info[k + 1];

                  indeces[k / 2] = index;
                  values[k / 2] = value;
                }
                testSetList.add(new SparseVec(features, indeces, values));
              });
      testSize = testSetList.size();
      LOGGER.info("Test size: {}, train size: {}", testSize, trainSize);
      correctTopics = trainingTopics.toArray(new String[0]);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Mx startMx() {
    return new VecBasedMx(allTopics.length, features);
  }

  private double accuracy(Mx newWeights) {
    Mx probs = MxTools.multiply(new SparseMx(testSetList.toArray(new SparseVec[0])), MxTools.transpose(newWeights));
    AtomicInteger truePositives = new AtomicInteger(0);
    IntStream.range(0, testSetList.size()).parallel().forEach(i -> {
      final int argmax = VecTools.argmax(probs.row(i));
      if (allTopics[argmax].equals(testTopics.get(i))) {
        truePositives.incrementAndGet();
      }
    });
    final double accuracy = truePositives.get() / (double) testSize;
    LOGGER.info("Accuracy {}", accuracy);
    return accuracy;
  }
  
  private double accuracySKLearn() {
    try {
      String[] commands = new String[]{
              "bash",
              "-c",
              "source ~/.bashrc; cd src/test/resources/; python sklearn_one_vs_rest_multiclass.py"
      };
      Process proc = Runtime.getRuntime().exec(commands);
      proc.waitFor();
      Scanner reader = new Scanner(new InputStreamReader(proc.getErrorStream()));
      while (reader.hasNextLine()) {
        LOGGER.error(reader.nextLine());
      }
      reader = new Scanner(new InputStreamReader(proc.getInputStream()));
      String next = reader.next();
      double nxt = Double.parseDouble(next);
      LOGGER.info("SKLearn accuracy: {}", nxt);
      return nxt;
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      return 0;
    }
  }

  private void biClassifierAccuracy(Vec localWeights, String topic) {
    int trues = 0;
    int truePositives = 0;
    int positives = 0;
    int ones = 0;
    for (int i = 0; i < testSize; i++) {
      double x = MathTools.sigmoid(VecTools.multiply(testSetList.get(i), localWeights));
      LOGGER.info("p = {}, y = {}", x, testTopics.get(i).equals(topic) ? 1 : 0);
      if ((2 * x > 1) == (testTopics.get(i).equals(topic))) {
        trues++;
      }
      if (2 * x > 1) {
        positives++;
        if (testTopics.get(i).equals(topic))
          truePositives++;
      }
      if (testTopics.get(i).equals(topic))
        ones++;
    }
    LOGGER.info("accuracy = {}", trues / (double) testSize);
    LOGGER.info("precision = {}", truePositives / (double) positives);
    LOGGER.info("recall = {}", truePositives / (double) ones);
  }

  @Test
  public void testFTRLProximalBinomial() {
    splitDatset(42);
    readTestTrain();

    LOGGER.info("Updating weights");
    BiClassifierOptimizer optimizer = FTRLProximalOptimizer.builder()
            .alpha(100)
            .beta(0.1)
            .lambda1(0.03)
            .lambda2(0.12)
            .build();

    String topic = "политика";

    final SparseMx trainingSet = new SparseMx(trainingSetList.toArray(new SparseVec[0]));
    int[] corrects = Arrays.stream(correctTopics).mapToInt(s -> s.equals(topic) ? 1 : 0).toArray();

    long kek = System.currentTimeMillis();
    Vec newWeights = optimizer.optimizeWeights(trainingSet, corrects, new SparseVec(trainingSet.columns()));
    kek = System.currentTimeMillis() - kek;
    LOGGER.info("Time in nanosec: {}", kek);

    biClassifierAccuracy(newWeights, topic);
  }

  @Test
  public void testFTRLProximalOneVsRest() {
    splitDatset(336);
    readTestTrain();

    LOGGER.info("Updating weights");
    BiClassifierOptimizer optimizer = FTRLProximalOptimizer.builder()
            .alpha(100)
            .beta(0.1)
            .lambda1(0.03)
            .lambda2(0.12)
            .build();

    final SparseMx trainingSet = new SparseMx(trainingSetList.toArray(new SparseVec[0]));

    long timeStart = System.currentTimeMillis();
    Mx newWeights = optimizer.optimizeOneVsRest(trainingSet, correctTopics, startMx(), allTopics);
    LOGGER.info("Execution time {}", System.currentTimeMillis() - timeStart);

    accuracy(newWeights);
    accuracySKLearn();
  }

  @Test
  public void testCompareOneVsRestAverageAndSKLearn() {
    Random random = new Random(42);
    BiClassifierOptimizer optimizer = FTRLProximalOptimizer.builder()
            .alpha(100)
            .beta(0.1)
            .lambda1(0.03)
            .lambda2(0.12)
            .build();

    double ourAcc = 0;
    double skAcc = 0;

    for (int i = 0; i < 10; i++) {
      splitDatset(random.nextInt(Integer.MAX_VALUE));
      readTestTrain();
      LOGGER.info("Updating weights");

      final SparseMx trainingSet = new SparseMx(trainingSetList.toArray(new SparseVec[0]));

      long timeStart = System.currentTimeMillis();
      Mx newWeights = optimizer.optimizeOneVsRest(trainingSet, correctTopics, startMx(), allTopics);
      LOGGER.info("Execution time {}", System.currentTimeMillis() - timeStart);

      ourAcc += accuracy(newWeights);
      skAcc += accuracySKLearn();
    }
    LOGGER.info("Average accuracy {}", ourAcc / 10);
    LOGGER.info("Average SKLearn accuracy {}", skAcc / 10);
  }

  @Test
  public void testCompareMultinomialAndOneVsRest() {
    Random random = new Random(42);

    double oneVsRestAcc = 0;
    double multinomialAcc = 0;

    Optimizer optimizer = FTRLProximalOptimizer.builder()
            .alpha(250)
            .beta(0.8)
            .lambda1(0.04)
            .lambda2(0.135)
            .build();

    BiClassifierOptimizer biOptimizer = FTRLProximalOptimizer.builder()
            .alpha(100)
            .beta(0.1)
            .lambda1(0.03)
            .lambda2(0.12)
            .build();

    for (int i = 0; i < 10; i++) {
      splitDatset(random.nextInt(Integer.MAX_VALUE));
      readTestTrain();

      final SparseMx trainingSet = new SparseMx(trainingSetList.toArray(new SparseVec[0]));

      long time = System.currentTimeMillis();
      Mx newWeights = optimizer.optimizeWeights(trainingSet, correctTopics, startMx(), allTopics);
      time = System.currentTimeMillis() - time;

      LOGGER.info("Time in milliseconds: {}", time);

      multinomialAcc += accuracy(newWeights);

      time = System.currentTimeMillis();
      newWeights = biOptimizer.optimizeOneVsRest(trainingSet, correctTopics, startMx(), allTopics);
      time = System.currentTimeMillis() - time;

      LOGGER.info("Time in milliseconds: {}", time);

      oneVsRestAcc += accuracy(newWeights);
    }

    LOGGER.info("Average multinomial accuracy: {}", multinomialAcc / 10.0);
    LOGGER.info("Average one vs rest accuracy: {}", oneVsRestAcc / 10.0);
  }

  @Test
  public void testCompareMultinomialAndSKLearn() {
    Random random = new Random(42);

    double ourAcc = 0;
    double skAcc = 0;

    for (int i = 0; i < 10; i++) {
      splitDatset(random.nextInt(Integer.MAX_VALUE));
      readTestTrain();

      LOGGER.info("Updating weights");
      Optimizer optimizer = FTRLProximalOptimizer.builder()
              .alpha(250)
              .beta(0.8)
              .lambda1(0.04)
              .lambda2(0.135)
              .build();

      final SparseMx trainingSet = new SparseMx(trainingSetList.toArray(new SparseVec[0]));
      long time = System.currentTimeMillis();
      Mx newWeights = optimizer.optimizeWeights(trainingSet, correctTopics, startMx(), allTopics);
      time = System.currentTimeMillis() - time;
      LOGGER.info("Time in milliseconds: {}", time);

      ourAcc += accuracy(newWeights);
      skAcc += accuracySKLearn();
    }
    LOGGER.info("Average accuracy {}", ourAcc / 10);
    LOGGER.info("Average SKLearn accuracy {}", skAcc / 10);
  }

  @Test
  public void testFTRLProximalMultinomial() {
    splitDatset(42);
    readTestTrain();

    LOGGER.info("Updating weights");
    Optimizer optimizer = FTRLProximalOptimizer.builder()
            .alpha(250)
            .beta(0.8)
            .lambda1(0.04)
            .lambda2(0.135)
            .build();

    final SparseMx trainingSet = new SparseMx(trainingSetList.toArray(new SparseVec[0]));
    long time = System.currentTimeMillis();
    Mx newWeights = optimizer.optimizeWeights(trainingSet, correctTopics, startMx(), allTopics);
    time = System.currentTimeMillis() - time;
    LOGGER.info("Time in millisec: {}", time);

    accuracy(newWeights);
  }
}
