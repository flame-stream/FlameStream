package com.spbsu.benchmark.flink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.jetbrains.annotations.Nullable;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoSocketSource extends RichSourceFunction<WikipediaPage> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(KryoSocketSource.class);

  private final String hostname;
  private final int port;

  @Nullable
  private transient Client client;

  public KryoSocketSource(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    client = new Client(1000, 200000);
    client.getKryo().register(WikipediaPage.class);
    ((Kryo.DefaultInstantiatorStrategy) client.getKryo().getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  @Override
  public void run(SourceContext<WikipediaPage> ctx) throws Exception {
    client.addListener(new Listener() {
      public void received(Connection connection, Object object) {
        ctx.collect((WikipediaPage) object);
      }
    });

    client.addListener(new Listener() {
      @Override
      public void disconnected(Connection connection) {
        LOG.info("DISCONNECTED");
        client.stop();
      }
    });

    LOG.info("Connecting to {}:{}", hostname, port);
    client.start();
    client.connect(5000, hostname, port);
    LOG.info("CONNECTED");
    client.run();
  }

  @Override
  public void cancel() {
    final Client theSocket = client;
    if (theSocket != null) {
      theSocket.close();
    }
  }
}
