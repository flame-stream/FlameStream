package com.spbsu.flamestream.example.bl.classifier;

import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.DataPoint;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.FTRLProximalOptimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.ModelState;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Optimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.sun.org.apache.xpath.internal.operations.Bool;
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
import java.util.stream.Collectors;
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
  private final List<DataPoint> trainingSetList = new ArrayList<>();
  private final List<DataPoint> testSetList = new ArrayList<>();
  private Boolean[] isTrain;
  private String[] allTopics;

  private Optimizer nonStreamingOptimizer;
  private FTRLProximalOptimizer optimizer;

  @BeforeClass
  public void beforeClass() {
    testSize = 5000;
    trainSize = 10000;
    predictor.init();
    allTopics = Arrays.stream(predictor.getTopics()).map(String::trim).map(String::toLowerCase).toArray(String[]::new);
    nonStreamingOptimizer = FTRLProximalOptimizer.builder()
            .alpha(250)
            .beta(0.82)
            .lambda1(0.03)
            .lambda2(0.134)
            .build(allTopics);

    optimizer = FTRLProximalOptimizer.builder()
            .alpha(300)
            .beta(0.1)
            .lambda1(0.0089)
            .lambda2(0.154)
            .build(allTopics);
  }

  @AfterClass
  public void afterClass() {
    new File(TMP_TEST_PATH).delete();
    new File(TMP_TRAIN_PATH).delete();
  }

  private String callPython(String pythonCommand) {
    String[] commands = new String[]{
            "bash",
            "-c",
            "source ~/.bashrc; cd src/test/resources/; python " + pythonCommand
    };

    try {
      Process proc = Runtime.getRuntime().exec(commands);
      proc.waitFor();
      Scanner reader = new Scanner(new InputStreamReader(proc.getErrorStream()));
      while (reader.hasNextLine()) {
        LOGGER.error(reader.nextLine());
      }
      reader = new Scanner(new InputStreamReader(proc.getInputStream()));
      StringBuilder builder = new StringBuilder();
      while (reader.hasNextLine()) {
        builder.append(reader.nextLine());
        builder.append(System.lineSeparator());
      }
      return builder.toString();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
      return "";
    }
  }

  @Test
  public void split() {
    splitDatsetWindow(42);
  }

  private void splitDatsetWindow(int seed) {
    splitDatsetWindow(seed, 5, "0.761");
  }

  private void splitDatsetWindow(int seed, int windowSize, String lam) {
    String pythonCommand = String.format("sklearn_split_dataset.py %d %d %d %d %d %s %s", trainSize, testSize, 1000, seed, windowSize, lam, "window");
    LOGGER.info(callPython(pythonCommand));
  }

  private void splitDatsetRandomComplete(int seed) {
    String pythonCommand = String.format("sklearn_split_dataset.py %d %d %d %d %d %s %s", trainSize, testSize, 1000, seed, 3, "0.09", "random_complete");
    LOGGER.info(callPython(pythonCommand));
  }

  private void splitDatsetOrderedComplete(int seed) {
    String pythonCommand = String.format("sklearn_split_dataset.py %d %d %d %d %d %s %s", trainSize, testSize, 1000, seed, 3, "0.09", "ordered_complete");
    LOGGER.info(callPython(pythonCommand));
  }

  private void readDatasetFromFileToList(String filename, List<DataPoint> dataset) throws IOException {
    final String[] trainInfo = Files.lines(new File(filename).toPath())
            .limit(1).toArray(String[]::new)[0].split(" ");
    int size = Integer.parseInt(trainInfo[0]);
    Files.lines(new File(filename).toPath())
            .skip(1)
            .limit(size)
            .forEach(line -> {
              final String[] tokens = line.split(",");
              String topic = tokens[0].trim().toLowerCase();
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
              dataset.add(new DataPoint(new SparseVec(features, indeces, values), topic));
            });
  }

  private void readTestTrain() {
    trainingSetList.clear();
    testSetList.clear();
    try {
      final String[] trainInfo = Files.lines(new File(TMP_TRAIN_PATH).toPath())
              .limit(1).toArray(String[]::new)[0].split(" ");
      trainSize = Integer.parseInt(trainInfo[0]);
      features = Integer.parseInt(trainInfo[1]);
      readDatasetFromFileToList(TMP_TRAIN_PATH, trainingSetList);

      readDatasetFromFileToList(TMP_TEST_PATH, testSetList);
      testSize = testSetList.size();

      LOGGER.info("Test size: {}, train size: {}", testSize, trainSize);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void readStreaming() {
    trainingSetList.clear();
    testSetList.clear();
    try {
      final String[] trainInfo = Files.lines(new File(TMP_TRAIN_PATH).toPath())
              .limit(1).toArray(String[]::new)[0].split(" ");
      trainSize = Integer.parseInt(trainInfo[0]);
      features = Integer.parseInt(trainInfo[1]);
      readDatasetFromFileToList(TMP_TRAIN_PATH, trainingSetList);

      String[] kek = Files.lines(new File(TMP_TRAIN_PATH).toPath()).skip(trainSize + 1).limit(1).toArray(String[]::new);
      isTrain = Arrays.stream(kek[0].split(",")).map(Integer::parseInt).map(x -> x == 1).toArray(Boolean[]::new);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private Mx startMx() {
    return new VecBasedMx(allTopics.length, features);
  }

  private double accuracy(Mx newWeights) {
    Mx probs = MxTools.multiply(new SparseMx(testSetList.stream().map(s -> (SparseVec) s.getFeatures()).toArray(SparseVec[]::new)), MxTools.transpose(newWeights));
    AtomicInteger truePositives = new AtomicInteger(0);
    IntStream.range(0, testSetList.size()).parallel().forEach(i -> {
      final int argmax = VecTools.argmax(probs.row(i));
      if (allTopics[argmax].equals(testSetList.get(i).getLabel())) {
        truePositives.incrementAndGet();
      }
    });
    final double accuracy = truePositives.get() / (double) testSize;
    LOGGER.info("Accuracy {}", accuracy);
    return accuracy;
  }

  private double trainAccuracy(Mx newWeights) {
    Mx probs = MxTools.multiply(new SparseMx(trainingSetList.stream().map(s -> (SparseVec) s.getFeatures()).toArray(SparseVec[]::new)), MxTools.transpose(newWeights));
    AtomicInteger truePositives = new AtomicInteger(0);
    IntStream.range(0, trainingSetList.size()).parallel().forEach(i -> {
      final int argmax = VecTools.argmax(probs.row(i));
      if (allTopics[argmax].equals(trainingSetList.get(i).getLabel())) {
        truePositives.incrementAndGet();
      }
    });
    final double accuracy = truePositives.get() / (double) trainSize;
    LOGGER.info("Accuracy {}", accuracy);
    return accuracy;
  }

  private double accuracySKLearn(String alpha) {
    final String pythonCommand = "sklearn_one_vs_rest_multiclass.py " + alpha;
    final double accuracy = Double.parseDouble(callPython(pythonCommand));
    LOGGER.info("SKLearn accuracy: {}", accuracy);
    return accuracy;
  }

  private double accuracySKLearn() {
    return accuracySKLearn("0.0000009");
  }

  @Test
  public void testSKLearnParams() {
    splitDatsetWindow(42);
    readTestTrain();

    LOGGER.info("Updating weights");

    accuracySKLearn("0.000004"); // 0.000003, 0.6756
  }

  @Test
  public void testCompareMultinomialAndSKLearnWindow() {
    Random random = new Random(42);

    double ourAcc = 0;
    double skAcc = 0;

    for (int i = 0; i < 10; i++) {
      splitDatsetWindow(random.nextInt(Integer.MAX_VALUE));
      readTestTrain();

      LOGGER.info("Updating weights");

      long time = System.currentTimeMillis();
      Mx newWeights = optimizer.optimizeWeights(trainingSetList, startMx());
      time = System.currentTimeMillis() - time;
      LOGGER.info("Execution time {}", time);

      ourAcc += accuracy(newWeights);
      skAcc += accuracySKLearn();
    }
    LOGGER.info("Average accuracy {}", ourAcc / 10);
    LOGGER.info("Average SKLearn accuracy {}", skAcc / 10);
  }

  @Test
  public void testCompareMultinomialAndSKLearnRandomComplete() {
    Random random = new Random(42);

    double ourAcc = 0;
    double skAcc = 0;

    for (int i = 0; i < 10; i++) {
      splitDatsetRandomComplete(random.nextInt(Integer.MAX_VALUE));
      readTestTrain();

      LOGGER.info("Updating weights");

      long time = System.currentTimeMillis();
      Mx newWeights = nonStreamingOptimizer.optimizeWeights(trainingSetList, startMx());
      time = System.currentTimeMillis() - time;
      LOGGER.info("Execution time {}", time);

      ourAcc += accuracy(newWeights);
      skAcc += accuracySKLearn("0.0000045");
    }
    LOGGER.info("Average accuracy {}", ourAcc / 10);
    LOGGER.info("Average SKLearn accuracy {}", skAcc / 10);
  }

  @Test
  public void testCompareMultinomialAndSKLearnOrderedComplete() {
    Random random = new Random(42);

    double ourAcc = 0;
    double skAcc = 0;

    for (int i = 0; i < 10; i++) {
      splitDatsetOrderedComplete(random.nextInt(Integer.MAX_VALUE));
      readTestTrain();

      LOGGER.info("Updating weights");

      long time = System.currentTimeMillis();
      Mx newWeights = nonStreamingOptimizer.optimizeWeights(trainingSetList, startMx());
      time = System.currentTimeMillis() - time;
      LOGGER.info("Execution time {}", time);

      ourAcc += accuracy(newWeights);
      skAcc += accuracySKLearn("0.0000045");
    }
    LOGGER.info("Average accuracy {}", ourAcc / 10);
    LOGGER.info("Average SKLearn accuracy {}", skAcc / 10);
  }

  @Test
  public void testFTRLProximalMultinomial() {

    long splitTime = System.currentTimeMillis();
    splitDatsetWindow(42, 5, "0.761"); // 5 0.761 0.6936
    splitTime = System.currentTimeMillis() - splitTime;
    LOGGER.info("Split time: {}",  splitTime);
    readTestTrain();

    LOGGER.info("Updating weights");

    long time = System.currentTimeMillis();
    Mx newWeights = optimizer.optimizeWeights(trainingSetList, startMx());
    time = System.currentTimeMillis() - time;
    LOGGER.info("Execution time {}", time);

    accuracy(newWeights);
    //accuracySKLearn();
  }

  @Test
  public void testSelectWindowSizeAndLambda() {
    List<Integer> windowSizes = IntStream.range(1, 51).filter(x -> x % 2 == 0).boxed().collect(Collectors.toList());
    //List<String> lams = Arrays.asList("0.5", "0.7", "0.9", "0.93", "0.99", "0.999", "0.9993", "0.9999", "0.99997", "0.999997");
    List<String> lams = Arrays.asList("0.5", "0.999997");
    double bestAcc = 0;
    int bestSz = 0;
    String bestLam = "";
    for (Integer sz: windowSizes) {
      for (String lam: lams) {
        LOGGER.info("Started split");
        splitDatsetWindow(42, sz, lam);
        readTestTrain();
        LOGGER.info("Test train read");
        Mx newWeights = optimizer.optimizeWeights(trainingSetList, startMx());
        double acc = accuracy(newWeights);
        if (acc > bestAcc) {
          bestSz = sz;
          bestLam = lam;
          bestAcc = acc;
        }
        LOGGER.info("Best size: {}, best damping factor: {}, best accuracy: {}", bestSz, bestLam, bestAcc);
      }
    }
  }

  @Test
  public void testSelectSKLearnAlpha() {
    List<String> alphas = Arrays.asList("0.0000008", "0.00000085", "0.00000095", "0.0000007", "0.00000075", "0.0000009");
    String optAlpha = "";
    double maxAcc = 0;
    splitDatsetWindow(42);
    readTestTrain();
    for (String alpha: alphas) {
      double acc = accuracySKLearn(alpha);
      if (acc > maxAcc) {
        optAlpha = alpha;
        maxAcc = acc;
      }
      LOGGER.info("Best alpha: {}", optAlpha);
    }
  }

  @Test
  public void testCompleteOrdered() {
    splitDatsetOrderedComplete(42);
    readTestTrain();

    Mx newWeights = nonStreamingOptimizer.optimizeWeights(trainingSetList, startMx());
    accuracy(newWeights);
  }

  @Test
  public void testChooseRegularisationParams() {
    splitDatsetWindow(42);
    readTestTrain();
    for (int i = 1; i <= 50; i++) {
      double lambda1 = 0.008 + 0.0001 * i;
      final Optimizer optimizer = FTRLProximalOptimizer.builder()
              .alpha(300)
              .beta(0.1)
              .lambda1(lambda1)
              .lambda2(0.156)
              .build(allTopics);
      Mx newWeights = optimizer.optimizeWeights(trainingSetList, startMx());
      accuracy(newWeights);
    }
  }

  private double streamingAccuracy() {
    ModelState state = new ModelState(startMx());
    int truePositives = 0;
    int count = 0;
    for (int i = 0; i < trainSize; i++) {
      if (isTrain[i]) {
        state = optimizer.step(trainingSetList.get(i), state);
      } else {
        LOGGER.info("test");
        Vec probs = MxTools.multiply(state.weights(), trainingSetList.get(i).getFeatures());
        int argmax = VecTools.argmax(probs);
        if (allTopics[argmax].equals(trainingSetList.get(i).getLabel())) {
          truePositives++;
        }
        count++;
      }
    }
    double accuracy = truePositives / (double) count;
    LOGGER.info("Accuracy: {}", accuracy);
    return accuracy;
  }

  @Test
  public void testStreaming() {
    splitDatsetWindow(42);
    readStreaming();

    streamingAccuracy();
  }
}
