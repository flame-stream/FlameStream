package com.spbsu.flamestream.example.bl.classifier;

import akka.actor.ActorSystem;
import com.expleague.commons.math.MathTools;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.graph.FlameMap;
import com.spbsu.flamestream.core.graph.Sink;
import com.spbsu.flamestream.core.graph.Source;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.LocalClusterRuntime;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFront;
import com.spbsu.flamestream.runtime.edge.akka.AkkaFrontType;
import com.spbsu.flamestream.runtime.edge.akka.AkkaRearType;
import com.spbsu.flamestream.runtime.utils.AwaitResultConsumer;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Math.abs;
import static java.lang.Math.max;
import static org.testng.Assert.assertTrue;

public class PredictorStreamTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PredictorStreamTest.class.getName());
  private static final String CNT_VECTORIZER_PATH = "src/main/resources/cnt_vectorizer";
  private static final String WEIGHTS_PATH = "src/main/resources/classifier_weights";
  private static final String PATH_TO_TEST_DATA = "src/test/resources/sklearn_prediction";

  @Test
  public void testStreamPredict() throws InterruptedException, TimeoutException {
    final List<Document> documents = new ArrayList<>();
    final List<String> texts = new ArrayList<>();
    final List<double[]> pyPredictions = new ArrayList<>();
    { //read data for test
      final SklearnSgdPredictor predictor = new SklearnSgdPredictor(CNT_VECTORIZER_PATH, WEIGHTS_PATH);
      try (BufferedReader br = new BufferedReader(new FileReader(new File(PATH_TO_TEST_DATA)))) {
        final double[] data = parseDoubles(br.readLine());
        final int testCount = (int) data[0];

        for (int i = 0; i < testCount; i++) {
          final double[] pyPrediction = parseDoubles(br.readLine());
          pyPredictions.add(pyPrediction);

          final String docText = br.readLine().toLowerCase();
          final Map<String, Double> tfIdf = new HashMap<>();
          final double[] tfidfFeatures = parseDoubles(br.readLine());
          texts.add(docText);
          SklearnSgdPredictor.text2words(docText).forEach(word -> {
            final int featureIndex = predictor.wordIndex(word);
            tfIdf.put(word, tfidfFeatures[featureIndex]);
          });
          final Document document = new Document(tfIdf);
          documents.add(document);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    final ActorSystem system = ActorSystem.create("testStand", ConfigFactory.load("remote"));
    try (final LocalClusterRuntime runtime = new LocalClusterRuntime.Builder().parallelism(2).build()) {
      try (final FlameRuntime.Flame flame = runtime.run(predictorGraph())) {
        final List<AkkaFront.FrontHandle<Document>> handles = flame.attachFront(
                "totalOrderFront",
                new AkkaFrontType<Document>(system)
        ).collect(Collectors.toList());
        for (int i = 1; i < handles.size(); i++) {
          handles.get(i).unregister();
        }
        final AkkaFront.FrontHandle<Document> sink = handles.get(0);

        final AwaitResultConsumer<Topic[]> consumer = new AwaitResultConsumer<>(documents.size());
        flame.attachRear("totalOrderRear", new AkkaRearType<>(system, Topic[].class))
                .forEach(r -> r.addListener(consumer));
        documents.forEach(sink);
        sink.unregister();
        consumer.await(10, TimeUnit.MINUTES);

        final List<Topic[]> result = consumer.result().collect(Collectors.toList());
        Assert.assertEquals(result.size(), documents.size());

        for (int i = 0; i < result.size(); i++) {
          double maxDiff = 0;
          for (int j = 0; j < pyPredictions.get(i).length; j++) {
            final double diff = abs(pyPredictions.get(i)[j] - result.get(i)[j].probability());
            maxDiff = max(diff, maxDiff);
          }
          assertTrue(maxDiff < MathTools.EPSILON);

          Arrays.sort(result.get(i));
          LOGGER.info("Doc: {}", texts.get(i));
          LOGGER.info("Max diff {} in predictions", maxDiff);
          LOGGER.info("Predict: {}", (Object) result.get(i));
          LOGGER.info("\n");
        }
      }
    }
    Await.ready(system.terminate(), Duration.Inf());
  }

  @SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
  private Graph predictorGraph() {
    final Source source = new Source();
    final Sink sink = new Sink();
    final SklearnSgdPredictor predictor = new SklearnSgdPredictor(CNT_VECTORIZER_PATH, WEIGHTS_PATH);
    final FlameMap<Document, Topic[]> classifierMap = new FlameMap<>(
            new ClassifierFunction(predictor),
            Document.class,
            new Runnable() {
              @Override
              public void run() {
                predictor.init();
              }
            }
    );
    return new Graph.Builder()
            .link(source, classifierMap)
            .link(classifierMap, sink)
            .colocate(source, classifierMap, sink)
            .build(source, sink);
  }

  private static class ClassifierFunction implements Function<Document, Stream<Topic[]>> {
    private final TopicsPredictor predictor;

    private ClassifierFunction(TopicsPredictor predictor) {
      this.predictor = predictor;
    }

    @Override
    public Stream<Topic[]> apply(Document document) {
      return Stream.of(new Topic[][]{predictor.predict(document)});
    }
  }

  private static double[] parseDoubles(String line) {
    return Arrays
            .stream(line.split(" "))
            .mapToDouble(Double::parseDouble)
            .toArray();
  }
}
