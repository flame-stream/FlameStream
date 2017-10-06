package com.spbsu.benchmark.flink;

import com.spbsu.benchmark.commons.LatencyMeasurer;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.utils.WikipediaPageIterator;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class InvertedIndexBench {
  private final String managerHostname;
  private final int managerPort;

  private final int sourcePort;

  private final String benchHostname;
  private final int benchPort;

  public InvertedIndexBench(String managerHostname,
                            int managerPort,
                            int sourcePort,
                            String benchHostname,
                            int benchPort) {
    this.managerHostname = managerHostname;
    this.managerPort = managerPort;
    this.sourcePort = sourcePort;
    this.benchHostname = benchHostname;
    this.benchPort = benchPort;
  }

  public static void main(String[] args) throws Exception {
    final Config load;
    if (args.length == 1) {
      final Path filename = Paths.get(args[0]);
      load = ConfigFactory.parseReader(Files.newBufferedReader(filename)).getConfig("benchmark");
    } else {
      load = ConfigFactory.load("bench.conf").getConfig("benchmark");
    }

    new InvertedIndexBench(
            load.getString("manager-hostname"),
            load.getInt("manager-port"),
            load.getInt("consumer-port"),
            load.getString("bench-hostname"),
            load.getInt("bench-port")
    ).run();
  }

  public void run() throws Exception {
    final StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .createRemoteEnvironment(managerHostname, managerPort);

    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(100, 0);

    final DataStream<WikipediaPage> source = environment.socketTextStream("localhost", sourcePort, "#$#")
            .map(value -> {
              final WikipediaPageIterator pageIterator = new WikipediaPageIterator(
                      new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8.name()))
              );
              final WikipediaPage page = pageIterator.next();
              return page;
            });

    final FlinkStream<WikipediaPage, InvertedIndexStream.Output> flinkStream = new InvertedIndexStream();
    //flinkStream.stream(source).writeToSocket(benchHostname, benchPort,

    environment.execute();
  }
}
