package com.spbsu.benchmark.flink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.spbsu.benchmark.commons.LatencyMeasurer;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexRemove;
import com.spbsu.flamestream.example.inverted_index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.inverted_index.utils.WikipeadiaInput;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

public final class FlinkBench {
  private final String managerHostname;
  private final int managerPort;

  private final String benchHostname;
  private final String inputFilePath;

  private final int sourcePort;
  private final int sinkPort;

  private final List<String> jars;
  private final int limit;
  private final int parallelism;

  private final long rate;

  public FlinkBench(int limit,
                    String managerHostname,
                    int managerPort,
                    String benchHostname,
                    int sourcePort,
                    int sinkPort,
                    List<String> jars,
                    int parallelism,
                    long rate,
                    String inputFilePath) {
    this.limit = limit;
    this.managerHostname = managerHostname;
    this.benchHostname = benchHostname;
    this.managerPort = managerPort;
    this.sourcePort = sourcePort;
    this.sinkPort = sinkPort;
    this.jars = new ArrayList<>(jars);
    this.inputFilePath = inputFilePath;
    this.parallelism = parallelism;
    this.rate = rate;
  }

  public static void main(String[] args) throws Exception {
    final Config load;
    if (args.length == 1) {
      final Path filename = Paths.get(args[0]);
      load = ConfigFactory.parseReader(Files.newBufferedReader(filename)).getConfig("benchmark");
    } else {
      load = ConfigFactory.load("flink-bench.conf").getConfig("benchmark");
    }

    new FlinkBench(
            load.getInt("limit"),
            load.getString("manager-hostname"),
            load.getInt("manager-port"),
            load.getString("bench-hostname"),
            load.getInt("source-port"),
            load.getInt("sink-port"),
            load.getStringList("jars"),
            load.getInt("parallelism"),
            load.getInt("rate"),
            load.hasPath("input-path") ? load.getString("input-path") : null).run();
  }

  public void run() throws Exception {
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(0, 0);

    final StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .createRemoteEnvironment(managerHostname, managerPort, jars.toArray(new String[jars.size()]));

    environment.setParallelism(parallelism);
    environment.setMaxParallelism(parallelism);

    final DataStream<WikipediaPage> source = environment
            .addSource(new KryoSocketSource(benchHostname, sourcePort))
            .setParallelism(parallelism)
            .shuffle();

    final DataStreamSink<InvertedIndexStream.Result> sink = new InvertedIndexStream().stream(source, parallelism)
            .addSink(new KryoSocketSink(benchHostname, sinkPort))
            .setParallelism(parallelism);

    final Stream<WikipediaPage> wikipediaInput = (inputFilePath == null ?
            WikipeadiaInput.dumpStreamFromResources("wikipedia/national_football_teams_dump.xml")
            : WikipeadiaInput.dumpStreamFromFile(inputFilePath)
    ).limit(limit);


    final Server producer = producer(latencyMeasurer, wikipediaInput);
    final Server consumer = consumer(latencyMeasurer);

    environment.execute("Joba");

    final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
    System.out.println(stat);

    producer.stop();
    consumer.stop();
  }

  private Server producer(LatencyMeasurer<Integer> measurer, Stream<WikipediaPage> input) throws IOException {
    final Server producer = new Server(20_000_000, 1000);
    producer.getKryo().register(WikipediaPage.class);
    ((Kryo.DefaultInstantiatorStrategy) producer.getKryo().getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    final List<Connection> connections = new ArrayList<>();

    new Thread(() -> {
      synchronized (connections) {
        try {
          connections.wait();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      input.forEach(page -> {
                synchronized (connections) {
                  try {
                    final Connection connection = connections
                            .get(ThreadLocalRandom.current().nextInt(connections.size()));
                    measurer.start(page.id());
                    connection.sendTCP(page);
                    Thread.sleep(rate);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }
              }
      );

      connections.forEach(Connection::close);
    }).start();

    producer.addListener(new Listener() {
      @Override
      public void connected(Connection connection) {
        synchronized (connections) {
          connections.add(connection);
          connections.notify();
        }
      }
    });

    producer.start();
    producer.bind(sourcePort);
    return producer;
  }

  private Server consumer(LatencyMeasurer<Integer> latencyMeasurer) throws IOException {
    final Server consumer = new Server(2000, 20_000_000);
    consumer.getKryo().register(InvertedIndexStream.Result.class);
    consumer.getKryo().register(WordIndexAdd.class);
    consumer.getKryo().register(WordIndexRemove.class);
    consumer.getKryo().register(long[].class);
    ((Kryo.DefaultInstantiatorStrategy) consumer.getKryo().getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    consumer.addListener(new Listener() {
      @Override
      public void disconnected(Connection connection) {
        System.out.println("Consumer has been disconnected " + connection);
        new RuntimeException().printStackTrace();
      }
    });

    consumer.addListener(new Listener() {
      @Override
      public void received(Connection connection, Object o) {
        if (o instanceof InvertedIndexStream.Result) {
          final WordIndexAdd wordIndexAdd = ((InvertedIndexStream.Result) o).wordIndexAdd();
          final int docId = IndexItemInLong.pageId(wordIndexAdd.positions()[0]);
          latencyMeasurer.finish(docId);
        }
      }
    });

    consumer.start();
    consumer.bind(sinkPort);

    return consumer;
  }
}
