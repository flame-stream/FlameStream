package com.spbsu.benchmark.flink.lenta;

import com.spbsu.flamestream.example.bl.text_classifier.LentaCsvTextDocumentsReader;
import com.spbsu.flamestream.example.bl.text_classifier.model.Prediction;
import com.spbsu.flamestream.example.bl.text_classifier.model.TextDocument;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Document;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.SklearnSgdPredictor;
import com.spbsu.flamestream.example.bl.text_classifier.ops.filtering.classifier.Topic;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class FlinkBenchTest {
  static private long blinkAtMillis;

  static class ExamplesTopicPredictor implements FlinkBench.SerializableTopicsPredictor {
    public static final SklearnSgdPredictor predictor = new SklearnSgdPredictor(
            "../../examples/src/main/resources/cnt_vectorizer",
            "../../examples/src/main/resources/classifier_weights"
    );

    static {
      predictor.init();
    }

    @Override
    public Topic[] predict(Document document) {
      return predictor.predict(document);
    }
  }

  static class CSVReaderSupplier implements Supplier<Reader>, Serializable {
    @Override
    public Reader get() {
      try {
        return new BufferedReader(new InputStreamReader(
                new FileInputStream("../../examples/src/main/resources/lenta/lenta-ru-news.csv"),
                StandardCharsets.UTF_8
        ));
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testPredictionDataStream() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(2000);
    env.setStateBackend(new FsStateBackend(new File("rocksdb").toURI(), true));
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 100));
    env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
    final long blinkPeriodMillis = 60000;
    CollectSink.values.clear();
    predictionDataStream(
            env.addSource(new CSVParserSourceFunction(new CSVReaderSupplier(), CSVFormat.DEFAULT))
                    .setParallelism(1)
                    .map(record ->
                            LentaCsvTextDocumentsReader.document(record.get(0), record.get(2), record.getRecordNumber())
                    )
    ).map(value -> {
      if (System.currentTimeMillis() > blinkAtMillis) {
        System.out.println("blink");
        blinkAtMillis = System.currentTimeMillis() + blinkPeriodMillis;
        throw new RuntimeException("blink");
      }
      return value;
    }).addSink(new CSVSink("predictions.csv")).setParallelism(1);
    blinkAtMillis = System.currentTimeMillis() + 14000;
    env.execute();
  }

  private DataStream<Prediction> predictionDataStream(DataStream<TextDocument> source) {
    return FlinkBench.predictionDataStream(new ExamplesTopicPredictor(), source);
  }

  private static class CollectSink implements SinkFunction<Prediction> {
    static final ArrayList<Prediction> values = new ArrayList<>();

    @Override
    public void invoke(Prediction value, Context context) throws Exception {
      values.add(value);
    }
  }

  private static class CSVSink implements SinkFunction<Prediction> {
    private final String fileName;
    private CSVPrinter openedCsvPrinter;
    private String[] topicNames;

    private CSVSink(String fileName) {this.fileName = fileName;}

    @Override
    public void invoke(Prediction prediction, Context context) throws Exception {
      if (topicNames == null) {
        topicNames = Stream.of(prediction.topics()).map(Topic::name).toArray(String[]::new);
        for (final String topicName : topicNames) {
          csvPrinter().print(topicName);
        }
      }
      csvPrinter().print(prediction.tfIdf().document());
      if (prediction.topics().length != topicNames.length) {
        throw new RuntimeException("different topics number");
      }
      int i = 0;
      for (final Topic topic : prediction.topics()) {
        if (!topic.name().equals(topicNames[i])) {
          throw new RuntimeException("inconsistent topics order");
        }
        i++;
        csvPrinter().print(topic.probability());
      }
      csvPrinter().println();
      if (prediction.tfIdf().number() % 1000 == 0) {
        System.out.println(prediction.tfIdf().number());
      }
    }

    private CSVPrinter csvPrinter() {
      if (openedCsvPrinter != null) {
        return openedCsvPrinter;
      }
      try {
        openedCsvPrinter = new CSVPrinter(new FileWriter(fileName), CSVFormat.DEFAULT);
        openedCsvPrinter.print("document");
        return openedCsvPrinter;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected void finalize() throws Throwable {
      if (openedCsvPrinter != null) {
        if (topicNames == null) {
          openedCsvPrinter.println();
        }
        openedCsvPrinter.close();
      }
    }
  }
}
