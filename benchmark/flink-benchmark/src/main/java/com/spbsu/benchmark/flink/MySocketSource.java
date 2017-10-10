package com.spbsu.benchmark.flink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.IOUtils;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MySocketSource extends RichSourceFunction<WikipediaPage> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(MySocketSource.class);

  private final String hostname;
  private final int port;

  private transient Socket currentSocket;
  private transient Kryo kryo = null;

  private volatile boolean isRunning = true;

  public MySocketSource(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    kryo = new Kryo();
    kryo.register(WikipediaPage.class);
    ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  @Override
  public void run(SourceContext<WikipediaPage> ctx) throws Exception {
    try (Socket socket = new Socket()) {
      currentSocket = socket;

      LOG.info("Connecting to server socket " + hostname + ':' + port);
      socket.connect(new InetSocketAddress(hostname, port), (int) SECONDS.toMillis(5));
      final Input input = new Input(socket.getInputStream());
      while (!input.eof() && isRunning) {
        final WikipediaPage wikipediaPage = kryo.readObject(input, WikipediaPage.class);
        ctx.collect(wikipediaPage);
      }
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
    final Socket theSocket = currentSocket;
    if (theSocket != null) {
      IOUtils.closeSocket(theSocket);
    }
  }
}
