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
import java.util.ArrayList;
import java.util.List;

public final class InvertedIndexBench {
  private final String managerHostname;
  private final int managerPort;

  private final String benchHostname;

  private final int sourcePort;
  private final int sinkPort;

  private final List<String> jars;

  public InvertedIndexBench(String managerHostname,
                            int managerPort,
                            String benchHostname,
                            int sourcePort,
                            int sinkPort,
                            List<String> jars) {
    this.managerHostname = managerHostname;
    this.benchHostname = benchHostname;
    this.managerPort = managerPort;
    this.sourcePort = sourcePort;
    this.sinkPort = sinkPort;
    this.jars = new ArrayList<>(jars);
  }

  public static void main(String[] args) throws Exception {
    final Config load;
    if (args.length == 1) {
      final Path filename = Paths.get(args[0]);
      load = ConfigFactory.parseReader(Files.newBufferedReader(filename)).getConfig("benchmark");
    } else {
      load = ConfigFactory.load("flink-bench.conf").getConfig("benchmark");
    }

    new InvertedIndexBench(
            load.getString("manager-hostname"),
            load.getInt("manager-port"),
            load.getString("bench-hostname"),
            load.getInt("source-port"),
            load.getInt("sink-port"),
            load.getStringList("jars")
    ).run();
  }


  public void run() throws Exception {
    final StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .createRemoteEnvironment(managerHostname, managerPort, jars.toArray(new String[jars.size()]));

    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(100, 0);

    final DataStream<WikipediaPage> source = environment.socketTextStream(benchHostname, sourcePort, "#$#")
            .map(value -> {
              final WikipediaPageIterator pageIterator = new WikipediaPageIterator(
                      new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8.name()))
              );
              final WikipediaPage page = pageIterator.next();
              return page;
            });

    final DataStream<InvertedIndexStream.Output> flinkStream = new InvertedIndexStream()
            .stream(source);

    environment.execute("Joba");
  }
}
