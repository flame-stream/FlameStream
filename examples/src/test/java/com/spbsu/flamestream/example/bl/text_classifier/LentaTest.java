package com.spbsu.flamestream.example.bl.text_classifier;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.text_classifier.model.containers.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.ops.classifier.*;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.jooq.lambda.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static org.testng.AssertJUnit.assertEquals;

public class LentaTest extends FlameAkkaSuite {
  private static final Logger LOGGER = LoggerFactory.getLogger(LentaTest.class);

  private Stream<TextDocument> documents(String path) throws IOException {
    final Reader reader = new InputStreamReader(
            LentaTest.class.getClassLoader().getResourceAsStream(path));
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
    return StreamSupport.stream(csvSpliterator, false).map(r -> {
      Pattern p = Pattern.compile("\\w+", Pattern.UNICODE_CHARACTER_CLASS);
      String recordText = r.get(2); // text order
      Matcher m = p.matcher(recordText);
      StringBuilder text = new StringBuilder();
      while (m.find()) {
        text.append(" ");
        text.append(m.group());
      }
      return new TextDocument(
              r.get(0), // url order
              text.substring(1).toLowerCase(),
              String.valueOf(ThreadLocalRandom.current().nextInt(0, 10)),
              counter.incrementAndGet(),
              r.get(4) // label
      );
    });

  }

  @Test
  public void lentaTest() throws TimeoutException, InterruptedException, IOException {
    final ActorSystem system = ActorSystem.create("lentaTfIdf", ConfigFactory.load("remote"));
    try (final LocalClusterRuntime runtime = new LocalClusterRuntime.Builder().maxElementsInGraph(10)
            .parallelism(2)
            .millisBetweenCommits(1000)
            .build()) {
      test(runtime, system);
    }
    Await.ready(system.terminate(), Duration.Inf());
  }

  @Test
  public void lentaBlinkTest() throws IOException, InterruptedException {
    try (final LocalRuntime runtime = new LocalRuntime.Builder().maxElementsInGraph(100)
            .parallelism(2)
            .withBlink()
            .blinkPeriodSec(7)
            .millisBetweenCommits(2000)
            .build()) {
      test(runtime, runtime.system());
    }
  }

  private TopicsPredictor initPredictor() {
    final String weightsPath = "src/main/resources/classifier_weights";
    return new SklearnSgdPredictor(weightsPath);
  }

  @Test
  public void onlineGraphValidation() throws IOException, InterruptedException, TimeoutException {
    final Graph onlineGraph = new TextClassifierGraph(initPredictor()).get();
    final ConcurrentLinkedDeque<Prediction> resultQueue = new ConcurrentLinkedDeque<>();
    final int parallelism = 2;
    final int totalDocuments = 2;//1000;

    final List<TextDocument> rawDocuments = documents("lenta/lenta-ru-news.csv")
            .limit(totalDocuments)
            .collect(toList());
    final List<TextDocument> labeledDocuments = rawDocuments
            .stream()
            .limit(totalDocuments / 2)
            .collect(toList());

    final Map<String, String> expectedLabels = new HashMap<>();
    final List<TextDocument> nonLabeledDocuments = rawDocuments
            .stream()
            .skip(totalDocuments / 2)
            .map(doc -> {
              expectedLabels.put(doc.name(), doc.label());
              return new TextDocument(doc.name(), doc.content(), doc.partitioning(), doc.number(), null);
            })
            .collect(toList());

    List<TextDocument> testDocuments = new ArrayList<>(labeledDocuments);
    testDocuments.addAll(nonLabeledDocuments);
    //Collections.shuffle(testDocuments);
    final ActorSystem system = ActorSystem.create("lentaTfIdf", ConfigFactory.load("remote"));

    final AtomicInteger counter = new AtomicInteger(0);
    try (final LocalClusterRuntime runtime = new LocalClusterRuntime.Builder().parallelism(parallelism).build()) {
      //ActorSystem system = runtime.system();
      try (final FlameRuntime.Flame flame = runtime.run(onlineGraph)) {
        flame.attachRear("tfidfRear", new AkkaRearType<>(system, Prediction.class))
                .forEach(r -> r.addListener(resultQueue::add));
        final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
                .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(system))
                .collect(toList());

        final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
        for (int i = 1; i < handles.size(); i++) {
          handles.get(i).unregister();
        }

        final Thread checker = new Thread(Unchecked.runnable(() -> {
          Prediction item = resultQueue.poll();
          while (item == null) {
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            item = resultQueue.poll();

            if (item != null) {
              counter.incrementAndGet();
              System.out.println(item.tfIdf().document());
              System.out.println(item.tfIdf().label());
            }
          }
        }));

        checker.start();
        testDocuments.forEach(front);
        checker.join();

        assertEquals(totalDocuments / 2, counter.get());
      }
    }
  }

  private void test(FlameRuntime runtime, ActorSystem system) throws IOException, InterruptedException {
    final String testFilePath = "lenta/lenta-ru-news.csv";
    final long expectedDocs = documents(testFilePath).count();
    final String weightsPath = "src/main/resources/classifier_weights";

    final TopicsPredictor predictor = new SklearnSgdPredictor(weightsPath);
    final ConcurrentLinkedDeque<Prediction> resultQueue = new ConcurrentLinkedDeque<>();

    try (final FlameRuntime.Flame flame = runtime.run(new TextClassifierGraph(predictor).get())) {
      flame.attachRear("tfidfRear", new AkkaRearType<>(system, Prediction.class))
              .forEach(r -> r.addListener(resultQueue::add));
      final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
              .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(system))
              .collect(toList());

      final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
      for (int i = 1; i < handles.size(); i++) {
        handles.get(i).unregister();
      }

      final AtomicInteger counter = new AtomicInteger(0);
      final Thread thread = new Thread(Unchecked.runnable(() -> {
        final Iterator<TextDocument> toCheckIter = documents(testFilePath).iterator();
        final Map<String, Integer> idfExpected2 = new HashMap<>();
        for (int i = 0; i < expectedDocs; i++) {
          Prediction prediction = resultQueue.poll();
          while (prediction == null) {
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            prediction = resultQueue.poll();
          }

          final int got = counter.incrementAndGet();
          if (got % 1000 == 0) {
            LOGGER.info(String.format("processed %s records", got));
          }

          final TextDocument processedDoc = toCheckIter.next();
          final List<String> pdWords = TextUtils.text2words(processedDoc.content()).collect(toList());
          final Set<String> pdWordsSet = TextUtils.text2words(processedDoc.content())
                  .collect(Collectors.toSet());
          pdWordsSet.forEach(w -> idfExpected2.merge(w, 1, Integer::sum));
          final TfIdfObject tfIdf = prediction.tfIdf();
          { //runtime info logging
            final Runtime rt = Runtime.getRuntime();
            if (got % 20 == 0) {
              LOGGER.info(
                      "pdWords: %d %d %d/%d %d %d %d %s%n",
                      tfIdf.number(),
                      got,
                      rt.freeMemory(),
                      rt.totalMemory(),
                      pdWords.size(),
                      idfExpected2.size(),
                      i,
                      pdWords
              );
            }
          }

          final Map<String, Integer> result = new HashMap<>();
          TextUtils.text2words(processedDoc.content()).forEach(w -> result.merge(w, 1, Integer::sum));
          Assert.assertEquals(processedDoc.name(), tfIdf.document(), String.format(
                  "unexpected document: '%s' instead of '%s'%n",
                  tfIdf.document(),
                  processedDoc.name()
          ));
          Assert.assertEquals(
                  result.keySet(),
                  tfIdf.words(),
                  String.format("unexpected keys: '%s' instead of '%s'%n", result.keySet(), tfIdf.words())
          );
          for (String key : result.keySet()) {
            Assert.assertEquals(
                    result.get(key).intValue(),
                    tfIdf.tf(key),
                    String.format("incorrect TF value for key %s: %d. Expected: %d%n",
                            key, tfIdf.tf(key), result.get(key)
                    )
            );
            Assert.assertEquals(
                    idfExpected2.get(key).intValue(),
                    tfIdf.idf(key),
                    String.format("incorrect IDF value for key %s: %d. Expected: %d%n",
                            key, tfIdf.idf(key), idfExpected2.get(key)
                    )
            );
          }

          final Topic[] topics = prediction.topics();
          Arrays.sort(topics);
          LOGGER.info("Doc: {}", processedDoc.content());
          LOGGER.info("Predict: {}", (Object) topics);
          LOGGER.info("\n");
        }
      }));

      thread.start();

      documents(testFilePath).forEach(front);

      thread.join();

      Assert.assertEquals(counter.get(), expectedDocs);
    }
  }
}
