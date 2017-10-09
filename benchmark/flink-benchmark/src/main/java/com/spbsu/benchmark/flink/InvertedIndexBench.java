package com.spbsu.benchmark.flink;

import com.fasterxml.jackson.core.JsonGenerator;
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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SocketClientSink;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.jooq.lambda.Unchecked;

import java.io.IOException;
import java.io.OutputStream;
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
    final LatencyMeasurer<Integer> latencyMeasurer = new LatencyMeasurer<>(10, 0);

    final StreamExecutionEnvironment environment = StreamExecutionEnvironment
            .createRemoteEnvironment(managerHostname, managerPort, jars.toArray(new String[jars.size()]));

    final DataStream<WikipediaPage> source = environment
            .socketTextStream(
                    benchHostname,
                    sourcePort,
                    "<delimiter />"
            )
            .map(new JacksonDeserial());

    new InvertedIndexStream().stream(source)
            .addSink(new SocketClientSink<>(benchHostname, sinkPort, new JacksonSchema<>()));


    final Stream<WikipediaPage> wikipeadiaInput = WikipeadiaInput
            .dumpStreamFromResources("wikipedia/national_football_teams_dump.xml")
            .peek(wikipediaPage -> latencyMeasurer.start(wikipediaPage.id()));

    new Thread(new Source(wikipeadiaInput)).start();
    new Thread(new Sink(latencyMeasurer)).start();

    environment.execute("Joba");

    final LongSummaryStatistics stat = Arrays.stream(latencyMeasurer.latencies()).summaryStatistics();
    System.out.println(stat);
  }

  private static final class JacksonSchema<T> implements SerializationSchema<T> {
    private static final long serialVersionUID = -9199833507306363333L;
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

  private static final class JacksonDeserial extends RichMapFunction<String, WikipediaPage> {
    private static final long serialVersionUID = -1909730945328813283L;
    private ObjectMapper mapper = null;

    @Override
    public void open(Configuration parameters) throws Exception {
      mapper = new ObjectMapper();
      super.open(parameters);
    }

    @Override
    public WikipediaPage map(String value) throws Exception {
      return mapper.readValue(value, WikipediaPage.class);
    }
  }

  private final class Source implements Runnable {
    private final Stream<WikipediaPage> input;

    private final ObjectMapper mapper = new ObjectMapper()
            .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    private Source(Stream<WikipediaPage> input) {
      this.input = input;
    }

    @Override
    public void run() {
      try (ServerSocket socket = new ServerSocket(sourcePort);
           Socket accept = socket.accept()) {
        final OutputStream outputStream = accept.getOutputStream();
        input.forEach(Unchecked.consumer(page -> {
          mapper.writeValue(outputStream, page);
          outputStream.write("<delimiter />".getBytes());
        }));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private final class Sink implements Runnable {
    private final ObjectMapper mapper = new ObjectMapper();
    private final LatencyMeasurer<Integer> latencyMeasurer;

    private Sink(LatencyMeasurer<Integer> latencyMeasurer) {
      this.latencyMeasurer = latencyMeasurer;
    }

    @Override
    public void run() {
      try (ServerSocket socket = new ServerSocket(sinkPort)) {
        while (true) {
          final Socket accept = socket.accept();

          new Thread(() -> {
            try {
              final MappingIterator<InvertedIndexStream.Output> iterator = mapper.reader()
                      .forType(InvertedIndexStream.Output.class)
                      .readValues(accept.getInputStream());

              while (iterator.hasNext()) {
                final WordIndexAdd wordIndexAdd = iterator.next().wordIndexAdd();
                final int docId = IndexItemInLong.pageId(wordIndexAdd.positions()[0]);
                latencyMeasurer.finish(docId);
              }

            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }).start();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
