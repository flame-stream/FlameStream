package com.spbsu.benchmark.flink.index;

import com.spbsu.benchmark.flink.index.ops.KryoSocketSink;
import com.spbsu.benchmark.flink.index.ops.KryoSocketSource;
import com.spbsu.benchmark.flink.index.ops.RichIndexFunction;
import com.spbsu.benchmark.flink.index.ops.WikipediaPageToWordPositions;
import com.spbsu.flamestream.example.benchmark.BenchStand;
import com.spbsu.flamestream.example.benchmark.GraphDeployer;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;

public final class FlinkBench {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkBench.class);

  public static void main(String[] args) throws Exception {
    final Config benchConfig;
    final Config deployerConfig;
    if (args.length == 2) {
      benchConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[0]))).getConfig("benchmark");
      deployerConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[1]))).getConfig("deployer");
    } else {
      benchConfig = ConfigFactory.load("flink-bench.conf").getConfig("benchmark");
      deployerConfig = ConfigFactory.load("flink-deployer.conf").getConfig("deployer");
    }
    final BenchStand.StandConfig standConfig = new BenchStand.StandConfig(benchConfig);

    final GraphDeployer deployer = new GraphDeployer() {
      @Override
      public void deploy() {
        final int paralellism = deployerConfig.getInt("paralellism");

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment
                //.createLocalEnvironment(1);
                .createRemoteEnvironment(
                        deployerConfig.getString("manager-hostname"),
                        deployerConfig.getInt("manager-port"),
                        paralellism,
                        deployerConfig.getString("uber-jar")
                );

        final DataStream<WikipediaPage> source = environment
                .addSource(new KryoSocketSource(standConfig.benchHost(), standConfig.frontPort()))
                .setParallelism(paralellism)
                .shuffle();

        source.flatMap(new WikipediaPageToWordPositions())
                .setParallelism(paralellism)
                .keyBy(0)
                .map(new RichIndexFunction())
                .setParallelism(paralellism)
                .addSink(new KryoSocketSink(standConfig.benchHost(), standConfig.rearPort()))
                .setParallelism(paralellism);
        new Thread(() -> {
          try {
            environment.execute();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }).start();
      }

      @Override
      public void close() {
        // It will close itself on completion
      }
    };

    try (BenchStand benchStand = new BenchStand(standConfig, deployer)) {
      benchStand.run();
    }
  }
}
