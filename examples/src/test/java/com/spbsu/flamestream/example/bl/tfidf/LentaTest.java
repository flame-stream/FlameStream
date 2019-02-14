package com.spbsu.flamestream.example.bl.tfidf;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.example.bl.tfidf.model.IDFData;
import com.spbsu.flamestream.example.bl.tfidf.model.Prediction;
import com.spbsu.flamestream.example.bl.tfidf.model.TFData;
import com.spbsu.flamestream.example.bl.tfidf.model.TfIdfObject;
import com.spbsu.flamestream.example.bl.tfidf.model.TextDocument;
import com.spbsu.flamestream.example.bl.tfidf.ops.filtering.classifier.Topic;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.acceptance.FlameAkkaSuite;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.QueuedConsumer;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class LentaTest extends FlameAkkaSuite {
  private static final Logger LOGGER = LoggerFactory.getLogger(LentaTest.class);

  private Stream<TextDocument> documents() throws IOException {
    ClassLoader classLoader = LentaTest.class.getClassLoader();
    Reader reader = new InputStreamReader(
            classLoader.getResourceAsStream("lenta/lenta-ru-news.csv"));
    CSVParser csvFileParser = new CSVParser(reader, CSVFormat.DEFAULT);

    Iterator<CSVRecord> iter = csvFileParser.iterator();
    iter.next(); // skip headers

    Spliterator<CSVRecord> csvSpliterator = Spliterators.spliteratorUnknownSize(csvFileParser.iterator(), Spliterator.IMMUTABLE);
    Stream<CSVRecord> records = StreamSupport.stream(csvSpliterator, false);

    final int DATASET_SIZE = 10000;
    AtomicInteger counter = new AtomicInteger(0);
    return records.map(r -> {
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
              counter.incrementAndGet()
      );
    }).limit(DATASET_SIZE);

  }

  @Test
  public void lentaTest() throws InterruptedException, IOException, TimeoutException {
    Stream<TextDocument> toCheck = documents();
    int nExpected = documents().toArray().length;
    final QueuedConsumer<Object> awaitConsumer = new QueuedConsumer<>(nExpected);

    ActorSystem system = ActorSystem.create("lentaTfIdf", ConfigFactory.load("remote"));
    try (final LocalClusterRuntime runtime = new LocalClusterRuntime.Builder().maxElementsInGraph(10)
            .parallelism(2)
            .millisBetweenCommits(1000)
            .build()) {

      final FlameRuntime.Flame flame = runtime.run(new TfIdfGraph().get());

      flame.attachRear("tfidfRear", new AkkaRearType<>(system, Object.class))
              .forEach(r -> r.addListener(awaitConsumer));
      final List<AkkaFront.FrontHandle<TextDocument>> handles = flame
              .attachFront("tfidfFront", new AkkaFrontType<TextDocument>(system))
              .collect(toList());

      final AkkaFront.FrontHandle<TextDocument> front = handles.get(0);
      for (int i = 1; i < handles.size(); i++) {
        handles.get(i).unregister();
      }

      final AtomicInteger counter = new AtomicInteger(0);
      final List<String> errors = new ArrayList<String>();
      Thread t = new Thread(() -> {
        Iterator<TextDocument> toCheckIter = toCheck.iterator();
        IDFData idfExpected = new IDFData();
        Queue q = awaitConsumer.queue();
        for (int i = 0; i < nExpected; i++) {
          Object o = q.poll();


          while (o == null) {
            try {
                 Thread.sleep(10);
            } catch (InterruptedException e) {}
            o = q.poll();
          }

          final int got = counter.incrementAndGet();
          if (got % 1000 == 0) {
            LOGGER.info(String.format("processed %s records", got));
          }

          if (o instanceof Prediction) {
            TextDocument processedDoc = toCheckIter.next();
            List<String> pdWords = TextUtils.words(processedDoc.content());
            idfExpected.addWords(new HashSet(pdWords));
            Runtime rt = Runtime.getRuntime();
            Prediction prediction = (Prediction) o;
            TfIdfObject tfIdf = prediction.tfIdf();

            if (prediction.topics() == null) {
              if (got % 20 == 0) {
                System.out.format(
                        "pdWords: %d %d %d/%d %d %d %d %s%n",
                        tfIdf.number(),
                        got,
                        rt.freeMemory(),
                        rt.totalMemory(),
                        pdWords.size(),
                        idfExpected.keys().size(),
                        i,
                        pdWords
                );
              }
              TFData pdTF = TextUtils.tfData(processedDoc.content());

              if (!Objects.equals(processedDoc.name(), tfIdf.document())) {
                errors.add(String.format(
                        "unexpected document: '%s' instead of '%s'%n",
                        tfIdf.document(),
                        processedDoc.name()
                ));
              }
              if (!Objects.equals(pdTF.keys(), tfIdf.tfKeys())) {
                errors.add(String.format("unexpected keys: '%s' instead of '%s'%n", pdTF.keys(), tfIdf.tfKeys()));
              }
              for (String key : pdTF.keys()) {
                if (pdTF.value(key) != tfIdf.tfCount(key)) {
                  errors.add(String.format("incorrect TF value for key %s: %d. Expected: %d%n",
                          key, tfIdf.tfCount(key), pdTF.value(key)));
                }
                if (idfExpected.value(key) != tfIdf.idfCount(key)) {
                  errors.add(String.format("incorrect IDF value for key %s: %d. Expected: %d%n",
                          key, tfIdf.idfCount(key), idfExpected.value(key)));
                }
              }
              if (errors.size() > 0) break;
            } else {
              // classifier results

              if (got % 1000 == 0) {
                final Topic[] topics = prediction.topics();
                Arrays.sort(topics);
                LOGGER.info("Doc: {}", processedDoc.content());
                LOGGER.info("Predict: {}", (Object) prediction);
                LOGGER.info("\n");
              }
            }
          } else {
            errors.add("unexpected: " + o);
          }

        }
      });

      t.start();

      documents().forEach(front);

      t.join();

      Assert.assertEquals(counter.get(), nExpected);
      Assert.assertEquals(errors.size(), 0);
    }
    Await.ready(system.terminate(), Duration.Inf());
  }
}
