package com.spbsu.benchmark.flink.sum;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.LongStream;

import static com.google.common.math.Quantiles.percentiles;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class SumBench implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SumBench.class);
  private final Map<Long, Long> latencies = Collections.synchronizedMap(new LinkedHashMap<>());

  private final AwaitCountConsumer awaitConsumer;

  private final Server producer;
  private final Server consumer;

  private final SumConfig sumConfig;

  public SumBench(SumConfig sumConfig) throws IOException {
    awaitConsumer = new AwaitCountConsumer(sumConfig.inputSize());
    this.sumConfig = sumConfig;
    this.producer = producer();
    this.consumer = consumer();
  }

  public void run() throws InterruptedException {
    awaitConsumer.await(60, TimeUnit.MINUTES);
    producer.close();
    consumer.close();
  }

  private Server producer() throws IOException {
    final LongStream input = LongStream.range(0, sumConfig.inputSize());

    final Server producer = new Server(1_000_000, 1000);
    ((Kryo.DefaultInstantiatorStrategy) producer.getKryo().getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
    producer.getKryo().register(Tuple2.class);

    final Connection[] connection = new Connection[2];
    new Thread(() -> {
      synchronized (connection) {
        try {
          connection.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      final int[] i = {0};
      input.forEach(num -> {
                synchronized (connection) {
                  final long a = ThreadLocalRandom.current().nextLong();
                  final long b = ThreadLocalRandom.current().nextLong();
                  latencies.put(a + b, System.nanoTime());
                  connection[0].sendTCP(new Tuple2<>(num, a));
                  connection[1].sendTCP(new Tuple2<>(num, b));
                  LOG.info("Sending: {}", i[0]++);
                  final long v = (long) (nextExp(1.0 / sumConfig.sleepBetweenDocs()) * 1.0e6);
                  LockSupport.parkNanos(v);
                }
              }
      );
    }).start();

    producer.addListener(new Listener() {
      @Override
      public void connected(Connection newConnection) {
        synchronized (connection) {
          LOG.info("There is new connection: {}", newConnection.getRemoteAddressTCP());
          if (connection[0] == null) {
            LOG.info("Accepting first connection: {}", newConnection.getRemoteAddressTCP());
            connection[0] = newConnection;
          } else if (connection[1] == null) {
            LOG.info("Accepting second connection: {}", newConnection.getRemoteAddressTCP());
            connection[1] = newConnection;
            connection.notify();
          } else {
            LOG.info("Closing connection {}", newConnection.getRemoteAddressTCP());
            newConnection.close();
          }
        }
      }
    });
    producer.start();
    producer.bind(sumConfig.frontPort());
    return producer;
  }

  private double nextExp(double lambda) {
    return StrictMath.log(1 - ThreadLocalRandom.current().nextDouble()) / -lambda;
  }

  private Server consumer() throws IOException {
    final Server consumer = new Server(2000, 1_000_000);
    consumer.getKryo().register(long.class);
    ((Kryo.DefaultInstantiatorStrategy) consumer.getKryo()
            .getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    consumer.addListener(new Listener() {
      @Override
      public void disconnected(Connection connection) {
        LOG.info("Consumer has been disconnected {}", connection);
      }
    });

    consumer.addListener(new Listener() {
      @Override
      public void received(Connection connection, Object o) {
        if (o instanceof Long) {
          final long sum = ((long) o);
          if (!latencies.containsKey(sum)) {
            throw new IllegalStateException();
          }
          latencies.put(sum, System.nanoTime() - latencies.get(sum));
          awaitConsumer.accept(o);
          if (awaitConsumer.got() % 100 == 0) {
            LOG.info("Progress: {}/{}", awaitConsumer.got(), awaitConsumer.expected());
          }
        }
      }
    });

    consumer.start();
    consumer.bind(sumConfig.rearPort());
    return consumer;
  }

  @Override
  public void close() {
    producer.stop();
    consumer.stop();

    final long[] l = latencies.values().stream().mapToLong(i -> i).toArray();
    LOG.info("Result: {}", Arrays.toString(l));
    final long[] skipped = latencies.values()
            .stream()
            .skip(200)
            .mapToLong(i -> i)
            .toArray();
    LOG.info("Median: {}", ((long) percentiles().index(50).compute(skipped)));
    LOG.info("75%: {}", ((long) percentiles().index(75).compute(skipped)));
    LOG.info("90%: {}", ((long) percentiles().index(90).compute(skipped)));
    LOG.info("99%: {}", ((long) percentiles().index(99).compute(skipped)));
  }

  public static class SumConfig {
    private final int sleepBetweenDocs;
    private final int inputSize;
    private final String benchHost;
    private final int frontPort;
    private final int rearPort;

    public SumConfig(Config config) {
      sleepBetweenDocs = config.getInt("sleep-between-docs-ms");
      inputSize = config.getInt("input-size");
      benchHost = config.getString("bench-host");
      frontPort = config.getInt("bench-source-port");
      rearPort = config.getInt("bench-sink-port");
    }

    public int sleepBetweenDocs() {
      return sleepBetweenDocs;
    }

    public int inputSize() {
      return inputSize;
    }

    public String benchHost() {
      return benchHost;
    }

    public int frontPort() {
      return frontPort;
    }

    public int rearPort() {
      return rearPort;
    }
  }

  public static void main(String[] args) throws Exception {
    final Config benchConfig;
    final Config deployerConfig;
    if (args.length == 2) {
      benchConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[0]))).getConfig("benchmark");
      deployerConfig = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[1]))).getConfig("deployer");
    } else {
      benchConfig = ConfigFactory.load("sum-flink-bench.conf").getConfig("benchmark");
      deployerConfig = ConfigFactory.load("sum-flink-deployer.conf").getConfig("deployer");
    }
    final SumConfig sumConfig = new SumConfig(benchConfig);

    try (final SumBench sumBench = new SumBench(sumConfig)) {
      final StreamExecutionEnvironment environment;
      if (deployerConfig.hasPath("remote")) {
        environment = StreamExecutionEnvironment.createRemoteEnvironment(
                deployerConfig.getString("remote.manager-hostname"),
                deployerConfig.getInt("remote.manager-port"),
                deployerConfig.getString("remote.uber-jar")
        );
      } else {
        environment = StreamExecutionEnvironment.createLocalEnvironment(4);
      }
      environment.setBufferTimeout(0);
      environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

      if (deployerConfig.getString("approach").equals("pessimistic")) {
        final DataStream<Tuple2<Long, Long>> source1 = environment.addSource(new SumSockerSource(
                sumConfig.benchHost(),
                sumConfig.frontPort()
        )).setParallelism(1);

        final DataStream<Tuple2<Long, Long>> source2 = environment.addSource(new SumSockerSource(
                sumConfig.benchHost(),
                sumConfig.frontPort()
        )).setParallelism(1);

        source1.join(source2).where(new KeySelector<Tuple2<Long, Long>, Long>() {
          @Override
          public Long getKey(Tuple2<Long, Long> s) throws Exception {return s.f0;}
        }).equalTo(new KeySelector<Tuple2<Long, Long>, Long>() {
          @Override
          public Long getKey(Tuple2<Long, Long> s) throws Exception {return s.f0;}
        })
                .window(SlidingEventTimeWindows.of(Time.milliseconds(1), Time.milliseconds(1)))
                .apply(new JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long>() {
                  @Override
                  public Long join(Tuple2<Long, Long> first, Tuple2<Long, Long> second) throws Exception {
                    return first.f1 + second.f1;
                  }
                })
                .addSink(new KryoSocketSink(sumConfig.benchHost(), sumConfig.rearPort()));
        new Thread(() -> {
          try {
            environment.execute();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }).start();
      }
      sumBench.run();
      System.exit(0);
    }
  }
}
