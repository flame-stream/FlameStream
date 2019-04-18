package com.spbsu.flamestream.example.bl.classifier;

import com.expleague.commons.math.MathTools;
import com.expleague.commons.math.vectors.Mx;
import com.expleague.commons.math.vectors.MxTools;
import com.expleague.commons.math.vectors.Vec;
import com.expleague.commons.math.vectors.VecTools;
import com.expleague.commons.math.vectors.impl.mx.SparseMx;
import com.expleague.commons.math.vectors.impl.mx.VecBasedMx;
import com.expleague.commons.math.vectors.impl.vectors.SparseVec;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.BiClassifierOptimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.FTRLProximalOptimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Optimizer;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import com.sun.tools.corba.se.idl.toJavaPortable.InterfaceGen;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static org.testng.Assert.assertTrue;

public class FTRLProximalTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(FTRLProximalTest.class.getName());
  private static final String CNT_VECTORIZER_PATH = "src/main/resources/cnt_vectorizer";
  private static final String WEIGHTS_PATH = "src/main/resources/classifier_weights";
  private static final String PATH_TO_TEST_DATA = "src/test/resources/sklearn_prediction";
  private static final String PATH_TO_DATA = "src/test/resources/news_lenta.csv";
  private static final String BI_CLASSIFIER_CANCER_DATA = "src/test/resources/cancer.csv";
  private static final String BI_CLASSIFIER_SPAM_DATA = "src/test/resources/spam.csv";
  private static int len;
  private static int testSize;
  private static int trainSize;

  private final List<String> topics = new ArrayList<>();
  private final List<String> texts = new ArrayList<>();
  private final List<SparseVec> mx = new ArrayList<>();
  private List<Document> documents = new ArrayList<>();
  private final SklearnSgdPredictor predictor = new SklearnSgdPredictor(CNT_VECTORIZER_PATH, WEIGHTS_PATH);
  private List<String> testTopics;
  private List<String> testTexts;
  private List<SparseVec> trainingSetList;
  private List<SparseVec> testSetList;
  private String[] correctTopics;
  private String[] allTopics;
  private Set<Integer> trainIndices;

  String randString() {
    Random random = new Random(System.currentTimeMillis());
    return random.ints(20, 97, 123).boxed().collect(Collector.of(
            StringBuilder::new,
            (StringBuilder sb, Integer x) -> sb.append((char) x.intValue()),
            StringBuilder::append
    )).toString();
  }

  @BeforeClass
  public void beforeClass() {
    testSize = 3000;
    trainSize = 10000;
    predictor.init();
    allTopics = Arrays.stream(predictor.getTopics()).map(String::trim).map(String::toLowerCase).toArray(String[]::new);
    String rand = randString();
    try {
      AtomicInteger index = new AtomicInteger(0);
      trainIndices = documents().map(x -> new TextDocument(
              x.name(),
              x.content(),
              x.topic(),
              x.partitioning(),
              index.getAndIncrement()
      )).limit(trainSize)
              .mapToInt(TextDocument::number)
              .boxed()
              .collect(Collectors.toSet());
      int totalSize = testSize + trainSize;
      trainSize = trainIndices.size();
      testSize = totalSize - trainSize;
      LOGGER.info("Test size: {}, train size: {}", testSize, trainSize);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeMethod
  public void before() {
    mx.clear();
    documents.clear();
    topics.clear();
    texts.clear();
  }

  private Stream<TextDocument> documents() throws IOException {
    final Reader reader = new InputStreamReader(new FileInputStream(PATH_TO_DATA), Charset.forName("UTF-8"));
    final CSVParser csvFileParser = new CSVParser(reader, CSVFormat.DEFAULT);
    { // skip headers
      Iterator<CSVRecord> iter = csvFileParser.iterator();
      iter.next();
    }

    final Spliterator<CSVRecord> csvSpliterator = Spliterators.spliteratorUnknownSize(
            csvFileParser.iterator(),
            Spliterator.IMMUTABLE
    );
    AtomicInteger counter = new AtomicInteger(0);
    List<TextDocument> documents = StreamSupport.stream(csvSpliterator, false).map(r -> new TextDocument(
            r.get(4), // url order
            r.get(1),
            r.get(0).trim().toLowerCase(),
            String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
            counter.incrementAndGet()
    ))
            .filter(doc -> !doc.topic().equals("все"))
            .filter(doc -> !doc.topic().equals(""))
            .limit(testSize + trainSize)
            .collect(Collectors.toList());
    //Collections.reverse(documents);
    //documents.sort(Comparator.comparing(TextDocument::name));
    return documents.stream();
  }

  private void calcCompleteIdf() {
    try {
      final int testCount = (int) documents().count();
      final int features = predictor.getWeights().columns();
      Map<String, Integer> idf = new HashMap<>();
      documents().forEach(doc -> {
        final String docText = doc.content();
        texts.add(docText);

        String topic = doc.topic();
        topics.add(topic);

        SklearnSgdPredictor.text2words(docText)
                .distinct()
                .filter(word -> predictor.wordIndex(word) > 0)
                .forEach(word -> idf.put(word, idf.getOrDefault(word, 0) + 1));
      });


      documents().forEach(doc -> {
        final String docText = doc.content();

        final Map<String, Long> cnt = SklearnSgdPredictor
                .text2words(docText)
                .filter(word -> predictor.wordIndex(word) > 0)
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()));

        final double sum = cnt.values()
                .stream()
                .mapToLong(Long::longValue)
                .sum();
        Map<String, Double> tfIdf = new HashMap<>();
        cnt.forEach((key, value) -> {
          final double tfIdfValue = (value / sum) * Math.log(testCount / (double) idf.get(key)) + 1;
          tfIdf.put(key, tfIdfValue);
        });

        final double norm = Math.sqrt(tfIdf.values()
                .stream()
                .mapToDouble(x -> x * x)
                .sum());

        tfIdf.forEach((s, v) -> tfIdf.put(s, v / norm));

        final Document document = new Document(tfIdf);
        documents.add(document);

        SparseVec vec = new SparseVec(features);
        tfIdf.forEach((word, value) -> vec.set(predictor.wordIndex(word), value));
        mx.add(vec);
      });
      //allTopics = documents().map(TextDocument::topic).map(String::trim).map(String::toLowerCase).distinct().toArray(String[]::new);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    len = topics.size();

    testTopics = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(topics::get).collect(Collectors.toList());
    testTexts = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(texts::get).collect(Collectors.toList());
    testSetList = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(mx::get).collect(Collectors.toList());
    documents = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(documents::get).collect(Collectors.toList());
    trainingSetList = IntStream.range(0, len).filter(trainIndices::contains).mapToObj(mx::get).collect(Collectors.toList());
    correctTopics = IntStream.range(0, len).filter(trainIndices::contains).mapToObj(topics::get).toArray(String[]::new);
  }

  /*private long time(String url) {
    String[] ls = url.substring(22, 31).split("/");
    int year = Integer.parseInt(ls[0]);
    int month = Integer.parseInt(ls[1]);
    int day = Integer.parseInt(ls[2]);
    Calendar c = Calendar.getInstance();
    c.set(year, month - 1, day, 0, 0);
    return c.getTime().getTime();
  }

  private void calcWindowIdf(int windowSizeDays, double lam) {
    try {

      final int testCount = (int) documents().count();
      final int features = predictor.getWeights().columns();
      Map<String, Double> idf = new HashMap<>();
      Map<String, Integer> df = new HashMap<>();

      AtomicLong currentDate = new AtomicLong(0);

      final int windowSize = windowSizeDays * 200;

      documents().forEach(doc -> {
        long date = time(doc.name());

        if (currentDate.get() == 0 || (date - currentDate.get() >= windowSizeDays * 24 * 3600000l)) {
          df.forEach((s, v) -> idf.put(s, (v + idf.getOrDefault(s, 0.0)) * lam));
          idf.forEach((s, v) -> {
            if (!df.containsKey(s)) {
              idf.put(s, v * lam);
            }
          });
          df.clear();
          currentDate.set(date);
        }
        final String docText = doc.content();
        texts.add(docText);

        String topic = doc.topic();
        topics.add(topic);

        SklearnSgdPredictor.text2words(docText)
                .distinct()
                .filter(word -> predictor.wordIndex(word) > 0)
                .forEach(word -> df.put(word, df.getOrDefault(word, 0) + 1));

        final Map<String, Long> cnt = SklearnSgdPredictor
                .text2words(docText)
                .filter(word -> predictor.wordIndex(word) > 0)
                .collect(Collectors.groupingBy(x -> x, Collectors.counting()));

        final double sum = cnt.values()
                .stream()
                .mapToLong(Long::longValue)
                .sum();
        Map<String, Double> tfIdf = new HashMap<>();
        cnt.forEach((key, value) -> {
          final double tfIdfValue = (value / sum) * Math.log(windowSize / (idf.getOrDefault(key, 0d) + df.getOrDefault(key, 0))) + 1;
          tfIdf.put(key, tfIdfValue);
        });

        final double norm = Math.sqrt(tfIdf.values()
                .stream()
                .mapToDouble(x -> x * x)
                .sum());

        tfIdf.forEach((s, v) -> tfIdf.put(s, v / norm));

        final Document document = new Document(tfIdf);
        documents.add(document);

        SparseVec vec = new SparseVec(features);
        tfIdf.forEach((word, value) -> vec.set(predictor.wordIndex(word), value));
        mx.add(vec);
      });
      //allTopics = documents().map(TextDocument::topic).map(String::trim).map(String::toLowerCase).distinct().toArray(String[]::new);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    len = topics.size();

    testTopics = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(topics::get).collect(Collectors.toList());
    testTexts = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(texts::get).collect(Collectors.toList());
    documents = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(documents::get).collect(Collectors.toList());
    trainingSetList = IntStream.range(0, len).filter(trainIndices::contains).mapToObj(mx::get).collect(Collectors.toList());
    correctTopics = IntStream.range(0, len).filter(trainIndices::contains).mapToObj(topics::get).toArray(String[]::new);
  }*/

  private void readCompleteIdf() {
    try (BufferedReader br = new BufferedReader(new FileReader(new File(PATH_TO_TEST_DATA)))) {
      final double[] data = parseDoubles(br.readLine());
      final int testCount = (int) data[0];
      final int features = (int) data[1];

      for (int i = 0; i < testCount; i++) {
        final String docText = br.readLine().toLowerCase();
        texts.add(docText);

        String topic = br.readLine();
        topics.add(topic);
        final double[] info = parseDoubles(br.readLine());
        final int[] indeces = new int[info.length / 2];
        final double[] values = new double[info.length / 2];
        for (int k = 0; k < info.length; k += 2) {
          final int index = (int) info[k];
          final double value = info[k + 1];

          indeces[k / 2] = index;
          values[k / 2] = value;
        }

        final Map<String, Double> tfIdf = new HashMap<>();
        SparseVec vec = new SparseVec(features, indeces, values);

        SklearnSgdPredictor.text2words(docText).forEach(word -> {
          final int featureIndex = predictor.wordIndex(word);
          tfIdf.put(word, vec.get(featureIndex));
        });
        final Document document = new Document(tfIdf);
        documents.add(document);

        mx.add(vec);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    len = topics.size();

    testTopics = topics.stream().skip(len - testSize).collect(Collectors.toList());
    testTexts = texts.stream().skip(len - testSize).collect(Collectors.toList());
    documents = documents.stream().skip(len - testSize).collect(Collectors.toList());
    testSetList = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(mx::get).collect(Collectors.toList());
    trainingSetList = IntStream.range(0, len).filter(trainIndices::contains).mapToObj(mx::get).collect(Collectors.toList());
    correctTopics = topics.stream().limit(trainSize).toArray(String[]::new);
  }

  private void readCancerFeatures() {
    try (BufferedReader br = new BufferedReader(new FileReader(new File(BI_CLASSIFIER_CANCER_DATA)))) {
      br.lines().skip(1).forEach(line -> {
        String[] tokens = line.split(",");
        topics.add(tokens[0]);
        final double[] values = Arrays.stream(tokens)
                .skip(1)
                .mapToDouble(Double::parseDouble)
                .toArray();

        final Map<String, Double> tfIdf = new HashMap<>();
        SparseVec vec = new SparseVec(values.length);
        for (int i = 0; i < values.length; i++) {
          vec.set(i, values[i]);
        }
        mx.add(vec);
      });

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    len = topics.size();

    allTopics = new String[]{"M", "B"};
    testTopics = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(topics::get).collect(Collectors.toList());
    //testTexts = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(texts::get).collect(Collectors.toList());
    testSetList = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(mx::get).collect(Collectors.toList());
    trainingSetList = IntStream.range(0, len).filter(trainIndices::contains).mapToObj(mx::get).collect(Collectors.toList());
    correctTopics = IntStream.range(0, len).filter(trainIndices::contains).mapToObj(topics::get).toArray(String[]::new);
  }

  private void readSpamFeatures() {
    try (BufferedReader br = new BufferedReader(new FileReader(new File(BI_CLASSIFIER_SPAM_DATA)))) {
      br.lines().skip(1).forEach(line -> {
        String[] tokens = line.split(",");
        topics.add(tokens[tokens.length - 1]);
        final double[] values = Arrays.stream(tokens)
                .limit(tokens.length - 1)
                .mapToDouble(Double::parseDouble)
                .toArray();

        final Map<String, Double> tfIdf = new HashMap<>();
        SparseVec vec = new SparseVec(values.length);
        for (int i = 0; i < values.length; i++) {
          vec.set(i, values[i]);
        }
        mx.add(vec);
      });

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    len = topics.size();

    allTopics = new String[]{"0", "1"};
    testTopics = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(topics::get).collect(Collectors.toList());
    //testTexts = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(texts::get).collect(Collectors.toList());
    testSetList = IntStream.range(0, len).filter(i -> !trainIndices.contains(i)).mapToObj(mx::get).collect(Collectors.toList());
    trainingSetList = IntStream.range(0, len).filter(trainIndices::contains).mapToObj(mx::get).collect(Collectors.toList());
    correctTopics = IntStream.range(0, len).filter(trainIndices::contains).mapToObj(topics::get).toArray(String[]::new);
  }

  private Mx startMx() {
    return new VecBasedMx(predictor.getWeights().rows(), predictor.getWeights().columns());
  }

  private double accuracy() {
    double truePositives = 0;
    for (int i = 0; i < testSize; i++) {
      String text = testTexts.get(i);
      String ans = testTopics.get(i);
      Document doc = documents.get(i);

      Topic[] prediction = predictor.predict(doc);

      Arrays.sort(prediction);
      if (ans.equals(prediction[0].name().trim().toLowerCase())) {
        truePositives++;
      }
      //LOGGER.info("Doc: {}", text);
      //LOGGER.info("Real answers: {}", ans);
      //LOGGER.info("Predict: {}", (Object) prediction);
      //LOGGER.info("\n");
    }

    double accuracy = truePositives / testSize;
    LOGGER.info("Accuracy: {}", accuracy);
    return accuracy;
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
  public void testBiClassifier() {
    //calcCompleteIdf();
    readSpamFeatures();
    FTRLProximalOptimizer optimizer = FTRLProximalOptimizer.builder()
            .alpha(0.01)
            .beta(1)
            .build();
    SparseMx trainingSet = new SparseMx(trainingSetList.toArray(new SparseVec[0]));

    String topic = allTopics[1];

    Vec localWeights = optimizer.optimizeWeights(
            trainingSet,
            Arrays.stream(correctTopics).mapToInt(s -> s.equals(topic) ? 1 : 0).toArray(),
            new SparseVec(trainingSet.columns())
    );

    biClassifierAccuracy(localWeights, topic);
  }

  @Test
  public void testFTRLProximalBinomial() {
    calcCompleteIdf();

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
    calcCompleteIdf();

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
    //predictor.updateWeights(newWeights);

    //accuracy();

    Mx probs = MxTools.multiply(new SparseMx(testSetList.toArray(new SparseVec[0])), MxTools.transpose(newWeights));
    int truePositives = 0;
    for (int i = 0; i < testSize; i++) {
      double mx = -1;
      int argmax = 0;
      for (int j = 0; j < newWeights.rows(); j++) {
        if (MathTools.sigmoid(probs.get(i, j)) > mx) {
          mx = MathTools.sigmoid(probs.get(i, j));
          argmax = j;
        }
      }
      if (allTopics[argmax].equals(testTopics.get(i))) {
        truePositives++;
      }
    }
    LOGGER.info("Accuracy {}", truePositives / (double) testSize);
  }

  @Test
  public void testFTRLProximal() {
    calcCompleteIdf();

    LOGGER.info("Updating weights");
    Optimizer optimizer = FTRLProximalOptimizer.builder()
            .alpha(100)
            .beta(0.1)
            .lambda1(0.07)
            .lambda2(0.12)
            .build();

    final SparseMx trainingSet = new SparseMx(trainingSetList.toArray(new SparseVec[0]));
    double kek = System.nanoTime();
    Mx newWeights = optimizer.optimizeWeights(trainingSet, correctTopics, startMx(), allTopics);
    kek = System.nanoTime() - kek;
    LOGGER.info("Time in nanosec: {}", kek);

    /*predictor.updateWeights(newWeights);

    accuracy();*/

    Mx probs = MxTools.multiply(new SparseMx(testSetList.toArray(new SparseVec[0])), MxTools.transpose(newWeights));
    int truePositives = 0;
    for (int i = 0; i < testSize; i++) {
      double mx = -1;
      int argmax = 0;
      for (int j = 0; j < newWeights.rows(); j++) {
        if (Math.exp(probs.get(i, j)) > mx) {
          mx = Math.exp(probs.get(i, j));
          argmax = j;
        }
      }
      if (allTopics[argmax].equals(testTopics.get(i))) {
        truePositives++;
      }
    }
    LOGGER.info("Accuracy {}", truePositives / (double) testSize);
  }

  private static double[] parseDoubles(String line) {
    return Arrays
            .stream(line.split(" "))
            .mapToDouble(Double::parseDouble)
            .toArray();
  }
}
