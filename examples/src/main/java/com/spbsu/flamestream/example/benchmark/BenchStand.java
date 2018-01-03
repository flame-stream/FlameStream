package com.spbsu.flamestream.example.benchmark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.example.bl.index.InvertedIndexGraph;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import com.spbsu.flamestream.example.bl.index.model.WordIndexAdd;
import com.spbsu.flamestream.example.bl.index.model.WordIndexRemove;
import com.spbsu.flamestream.example.bl.index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.bl.index.utils.WikipeadiaInput;
import com.spbsu.flamestream.runtime.LocalRuntime;
import com.spbsu.flamestream.runtime.edge.socket.SocketFrontType;
import com.spbsu.flamestream.runtime.edge.socket.SocketRearType;
import com.spbsu.flamestream.runtime.utils.AwaitCountConsumer;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 28.12.2017
 */
public class BenchStand implements AutoCloseable {
  private final static Logger LOG = LoggerFactory.getLogger(BenchStand.class);
  private final Map<Integer, LatencyMeasurer> latencies = new ConcurrentSkipListMap<>();

  private final Config config;
  private final BenchValidator<WordIndexAdd> validator;
  private final GraphDeployer graphDeployer;
  private final AwaitCountConsumer awaitConsumer;

  private final Server producer;
  private final Server consumer;

  public BenchStand(Config config, GraphDeployer graphDeployer) {
    this.config = config;
    this.graphDeployer = graphDeployer;
    try {
      //noinspection unchecked
      validator = ((Class<? extends BenchValidator>) Class.forName(config.validatorClass)).newInstance();
      awaitConsumer = new AwaitCountConsumer(validator.expectedOutputSize());

      producer = producer();
      consumer = consumer();
    } catch (IOException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public void run() throws IOException, InterruptedException {
    graphDeployer.deploy();
    awaitConsumer.await(5, TimeUnit.MINUTES);
  }

  private Server producer() throws IOException {
    final Stream<WikipediaPage> input = WikipeadiaInput.dumpStreamFromResources(config.wikiDumpPath)
            .limit(validator.inputLimit());
    final Server producer = new Server(1_000_000, 1000);
    producer.getKryo().register(WikipediaPage.class);
    ((Kryo.DefaultInstantiatorStrategy) producer.getKryo().getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    final Connection[] connection = new Connection[1];
    new Thread(() -> {
      synchronized (connection) {
        try {
          connection.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      input.forEach(page -> {
                synchronized (connection) {
                  try {
                    latencies.put(page.id(), new LatencyMeasurer());
                    connection[0].sendTCP(page);
                    LOG.info("Sending: {} at {}", page.id(), System.nanoTime());
                    Thread.sleep(config.sleepBetweenDocs);
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }
              }
      );
      connection[0].close();
    }).start();

    producer.addListener(new Listener() {
      @Override
      public void connected(Connection newConnection) {
        synchronized (connection) {
          if (connection[0] == null && newConnection.getRemoteAddressTCP().getHostName().equals(config.benchHost)) {
            connection[0] = newConnection;
            connection.notify();
          } else {
            newConnection.close();
          }
        }
      }
    });
    producer.start();
    producer.bind(config.frontPort);
    return producer;
  }

  private Server consumer() throws IOException {
    final Server consumer = new Server(2000, 1_000_000);
    { //register inners of data item
      consumer.getKryo().register(PayloadDataItem.class);
      consumer.getKryo().register(Meta.class);
      consumer.getKryo().register(GlobalTime.class);
      consumer.getKryo().register(EdgeId.class);
      consumer.getKryo().register(int[].class);
    }
    consumer.getKryo().register(WordIndexAdd.class);
    consumer.getKryo().register(WordIndexRemove.class);
    consumer.getKryo().register(long[].class);
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
        if (o instanceof DataItem) {
          final DataItem dataItem = (DataItem) o;
          final WordIndexAdd wordIndexAdd = dataItem.payload(WordIndexAdd.class);
          final int docId = IndexItemInLong.pageId(wordIndexAdd.positions()[0]);
          latencies.get(docId).finish();
          validator.accept(wordIndexAdd);
          awaitConsumer.accept(o);
        }
      }
    });

    consumer.start();
    consumer.bind(config.rearPort);
    return consumer;
  }

  @Override
  public void close() {
    validator.stop();
    graphDeployer.close();
    producer.stop();
    consumer.stop();
    { //print latencies
      final LongSummaryStatistics result = new LongSummaryStatistics();
      final StringBuilder stringBuilder = new StringBuilder();
      latencies.values().forEach(latencyMeasurer -> {
        result.accept(latencyMeasurer.statistics().getMax());
        stringBuilder.append(latencyMeasurer.statistics().getMax()).append(", ");
      });
      stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length()); //remove last ", "

      LOG.info("Latencies dump: {}", stringBuilder);
      LOG.info("Result: {}", result);
    }
  }

  public static class Config {
    @JsonProperty
    private final int sleepBetweenDocs;
    @JsonProperty
    private final String wikiDumpPath;
    @JsonProperty
    private final String validatorClass;
    @JsonProperty
    private final String benchHost;
    @JsonProperty
    private final int frontPort;
    @JsonProperty
    private final int rearPort;

    @JsonCreator
    public Config(@JsonProperty("sleepBetweenDocs") int sleepBetweenDocs,
                  @JsonProperty("wikiDumpPath") String wikiDumpPath,
                  @JsonProperty("validatorClass") String validatorClass,
                  @JsonProperty("benchHost") String benchHost,
                  @JsonProperty("frontPort") int frontPort,
                  @JsonProperty("rearPort") int rearPort) {
      this.sleepBetweenDocs = sleepBetweenDocs;
      this.wikiDumpPath = wikiDumpPath;
      this.validatorClass = validatorClass;
      this.benchHost = benchHost;
      this.frontPort = frontPort;
      this.rearPort = rearPort;
    }
  }

  //to check that it works
  public static void main(String[] args) throws IOException, InterruptedException {
    final Config config = new Config(
            100,
            "wikipedia/national_football_teams_dump.xml",
            "com.spbsu.flamestream.example.benchmark.validators.FootballTeamsValidator",
            "127.0.0.1",
            4567,
            5678
    );

    final GraphDeployer graphDeployer = new FlameGraphDeployer(
            new LocalRuntime(4),
            new InvertedIndexGraph().get(),
            new SocketFrontType(config.benchHost, config.frontPort, WikipediaPage.class),
            new SocketRearType(
                    config.benchHost,
                    config.rearPort,
                    WordIndexAdd.class,
                    WordIndexRemove.class,
                    long[].class
            )
    );
    try (final BenchStand benchStand = new BenchStand(config, graphDeployer)) {
      benchStand.run();
    }
  }
}
