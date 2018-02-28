package com.spbsu.benchmark.flink.index;

import com.spbsu.benchmark.flink.index.ops.IndexFunction;
import com.spbsu.benchmark.flink.index.ops.KryoSocketSink;
import com.spbsu.benchmark.flink.index.ops.KryoSocketSource;
import com.spbsu.benchmark.flink.index.ops.OrderEnforcer;
import com.spbsu.benchmark.flink.index.ops.TotalOrderEnforcer;
import com.spbsu.benchmark.flink.index.ops.WikipediaPageToWordPositions;
import com.spbsu.flamestream.example.benchmark.BenchStand;
import com.spbsu.flamestream.example.benchmark.GraphDeployer;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jooq.lambda.Unchecked;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FlinkBench {
  public static void main(String[] args) throws IOException, InterruptedException {
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
        final int parallelism = deployerConfig.getInt("parallelism");
        final StreamExecutionEnvironment environment;
        if (deployerConfig.hasPath("remote")) {
          environment = StreamExecutionEnvironment.createRemoteEnvironment(
                  deployerConfig.getString("remote.manager-hostname"),
                  deployerConfig.getInt("remote.manager-port"),
                  parallelism,
                  deployerConfig.getString("remote.uber-jar")
          );
        } else {
          environment = StreamExecutionEnvironment.createLocalEnvironment(parallelism);
        }
        environment.setBufferTimeout(0);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        environment
                .addSource(new KryoSocketSource(standConfig.benchHost(), standConfig.frontPort()))
                .setParallelism(parallelism)
                .shuffle()
                .flatMap(new WikipediaPageToWordPositions())
                .setParallelism(parallelism)
                .keyBy(0)
                .process(new OrderEnforcer())
                .process(new IndexFunction())
                .setParallelism(parallelism)
                //.keyBy((KeySelector<Result, Integer>) value
                //        -> IndexItemInLong.pageId(value.wordIndexAdd().positions()[0]))
                //.process(new TotalOrderEnforcer())
                .setParallelism(parallelism)
                .addSink(new KryoSocketSink(standConfig.benchHost(), standConfig.rearPort()))
                .setParallelism(parallelism);
        new Thread(Unchecked.runnable(environment::execute)).start();
        
      }

      @Override
      public void close() {
        // It will close itself on completion
      }
    };

    try (BenchStand benchStand = new BenchStand(standConfig, deployer)) {
      benchStand.run();
    }
    System.exit(0);
  }
}
