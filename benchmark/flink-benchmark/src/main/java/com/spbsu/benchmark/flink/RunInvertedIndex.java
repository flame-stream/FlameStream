package com.spbsu.benchmark.flink;

import com.spbsu.benchmark.commons.LatencyMeasurer;
import com.spbsu.benchmark.flink.streams.inverted_index.InvertedIndexFold;
import com.spbsu.benchmark.flink.streams.inverted_index.InvertedIndexStream;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public class RunInvertedIndex {
  public static void main(String[] args) {
    // TODO: 05.10.2017 parse arguments

    final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(100, 0);

    // TODO: 05.10.2017 parse socket
    final DataStream<WikipediaPage> source = null;// = environment.socketTextStream()
    final FlinkStream<WikipediaPage, InvertedIndexFold> flinkStream = new InvertedIndexStream();

    flinkStream.stream(source).addSink(value -> {
      // TODO: 05.10.2017 measure latency
    });
  }
}
