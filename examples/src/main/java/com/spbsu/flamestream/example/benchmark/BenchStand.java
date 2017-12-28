package com.spbsu.flamestream.example.benchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.bl.index.utils.WikipeadiaInput;
import com.spbsu.flamestream.runtime.utils.AwaitConsumer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class BenchStand {

  public static void main(String[] args) throws IOException, InterruptedException {
    final Config config = new Config(
            10,
            "wikipedia/national_football_teams_dump.xml",
            300,
            65813,
            4567,
            5678
    );
    final BenchStand benchStand = new BenchStand();
    benchStand.run(config, new LocalGraphDeployer(1, 1));
  }

  public void run(Config config, GraphDeployer graphDeployer) throws IOException, InterruptedException {
    final Map<Integer, LatencyMeasurer> latencies = new ConcurrentSkipListMap<>();
    final Server producer = producer(config, latencies);

    final AwaitConsumer<Object> awaitConsumer = new AwaitConsumer<>(config.expectedOutput);
    final Server consumer = consumer(config, latencies, awaitConsumer);

    graphDeployer.deploy();
    awaitConsumer.await(5, TimeUnit.MINUTES);
    graphDeployer.stop();

    producer.stop();
    consumer.stop();
  }

  private Server producer(Config config, Map<Integer, LatencyMeasurer> latencies) throws IOException {
    final Stream<WikipediaPage> input = WikipeadiaInput.dumpStreamFromResources(config.wikiDumpPath)
            .limit(config.inputLimit);
    final Server producer = new Server(1_000_000, 1000);
    producer.getKryo().register(WikipediaPage.class);
    ((Kryo.DefaultInstantiatorStrategy) producer.getKryo().getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

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
                    latencies.put(page.id(), new LatencyMeasurer());
                    connection.sendTCP(page);
                    System.out.println("Sending: " + page.id() + " at " + System.nanoTime());
                    Thread.sleep(config.sleepBetweenDocs);
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
    producer.bind(config.frontPort);
    return producer;
  }

  private Server consumer(Config config, Map<Integer, LatencyMeasurer> latencies, AwaitConsumer<Object> awaitConsumer)
          throws IOException {
    final Server consumer = new Server(2000, 1_000_000);
    consumer.getKryo().register(WordIndexAdd.class);
    consumer.getKryo().register(WordIndexRemove.class);
    consumer.getKryo().register(long[].class);
    ((Kryo.DefaultInstantiatorStrategy) consumer.getKryo()
            .getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

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
        if (o instanceof WordIndexAdd) {
          final WordIndexAdd wordIndexAdd = (WordIndexAdd) o;
          final int docId = IndexItemInLong.pageId(wordIndexAdd.positions()[0]);
          latencies.get(docId).finish();
        }
        awaitConsumer.accept(o);
      }
    });

    consumer.start();
    consumer.bind(config.rearPort);
    return consumer;
  }

  public static class Config {
    @JsonProperty
    private final int sleepBetweenDocs;
    @JsonProperty
    private final String wikiDumpPath;
    @JsonProperty
    private final int inputLimit;
    @JsonProperty
    private final int expectedOutput;
    @JsonProperty
    private final int frontPort;
    @JsonProperty
    private final int rearPort;

    @JsonCreator
    public Config(@JsonProperty("sleepBetweenDocs") int sleepBetweenDocs,
                  @JsonProperty("wikiDumpPath") String wikiDumpPath,
                  @JsonProperty("inputLimit") int inputLimit,
                  @JsonProperty("expectedOutput") int expectedOutput,
                  @JsonProperty("frontPort") int frontPort,
                  @JsonProperty("rearPort") int rearPort) {
      this.sleepBetweenDocs = sleepBetweenDocs;
      this.wikiDumpPath = wikiDumpPath;
      this.inputLimit = inputLimit;
      this.expectedOutput = expectedOutput;
      this.frontPort = frontPort;
      this.rearPort = rearPort;
    }
  }
}
