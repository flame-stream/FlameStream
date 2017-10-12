package com.spbsu.benchmark.flink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.benchmark.commons.LatencyMeasurer;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.utils.IndexItemInLong;
import com.spbsu.flamestream.example.inverted_index.utils.WikipeadiaInput;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.stream.Stream;

public final class FlinkBench {
  private final String managerHostname;
  private final int managerPort;

  private final String benchHostname;
  private final String inputFilePath;

  private final int sourcePort;
  private final int sinkPort;

  private final List<String> jars;

  public FlinkBench(String managerHostname,
                    int managerPort,
                    String benchHostname,
                    int sourcePort,
                    int sinkPort,
                    List<String> jars,
                    String inputFilePath) {
    this.managerHostname = managerHostname;
    this.benchHostname = benchHostname;
    this.managerPort = managerPort;
    this.sourcePort = sourcePort;
    this.sinkPort = sinkPort;
    this.jars = new ArrayList<>(jars);
    this.inputFilePath = inputFilePath;
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
            load.getString("manager-hostname"),
            load.getInt("manager-port"),
            load.getString("bench-hostname"),
            load.getInt("source-port"),
            load.getInt("sink-port"),
            load.getStringList("jars"),
            load.hasPath("input-path") ? load.getString("input-path") : null).run();
  }

  public void run() throws Exception {
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(10, 0);

    final StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .createLocalEnvironment(1);
    //.createRemoteEnvironment(managerHostname, managerPort, jars.toArray(new String[jars.size()]));

    final DataStream<WikipediaPage> source = environment
            .addSource(new MySocketSource(benchHostname, sourcePort)).setParallelism(1);

    new InvertedIndexStream().stream(source)
            .addSink(new SocketClientSink<>(benchHostname, sinkPort, new JacksonSchema<>()));

    final Stream<WikipediaPage> wikipediaInput = (
            inputFilePath == null ?
                    WikipeadiaInput.dumpStreamFromResources("wikipedia/national_football_teams_dump.xml")
                    : WikipeadiaInput.dumpStreamFromFile(inputFilePath)
    ).peek(wikipediaPage -> latencyMeasurer.start(wikipediaPage.id()));

    final Server server = new Server(200000, 1000);
    server.getKryo().register(WikipediaPage.class);
    ((Kryo.DefaultInstantiatorStrategy) server.getKryo().getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    server.addListener(new Listener() {
      @Override
      public void connected(Connection connection) {
        wikipediaInput.forEach(page -> {
                  connection.sendTCP(page);
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                }
        );
        connection.close();
      }
    });

    server.start();
    server.bind(sourcePort);

    final Thread consumer = new Thread(new Consumer(latencyMeasurer));
    consumer.setDaemon(true);
    consumer.start();

    environment.execute("Joba");

    final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
    System.out.println(stat);
    server.stop();
  }

  private static final class JacksonSchema<T> implements SerializationSchema<T> {
    private static final long serialVersionUID = 1L;

    private ObjectMapper mapper = null;

    @Override
    public byte[] serialize(T element) {
      if (mapper == null) {
        mapper = new ObjectMapper();
      }

      try {
        return mapper.writeValueAsBytes(element);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private final class Consumer implements Runnable {
    private final ObjectMapper mapper = new ObjectMapper();
    private final LatencyMeasurer<Integer> latencyMeasurer;

    private Consumer(LatencyMeasurer<Integer> latencyMeasurer) {
      this.latencyMeasurer = latencyMeasurer;
    }

    @Override
    public void run() {
      try (ServerSocket socket = new ServerSocket(sinkPort)) {
        while (true) {
          final Socket accept = socket.accept();

          final Thread worker = new Thread(() -> {
            try {
              final MappingIterator<InvertedIndexStream.Result> iterator = mapper.reader()
                      .forType(InvertedIndexStream.Result.class)
                      .readValues(accept.getInputStream());

              while (iterator.hasNext()) {
                final WordIndexAdd wordIndexAdd = iterator.next().wordIndexAdd();
                final int docId = IndexItemInLong.pageId(wordIndexAdd.positions()[0]);
                latencyMeasurer.finish(docId);
              }
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
          worker.start();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
