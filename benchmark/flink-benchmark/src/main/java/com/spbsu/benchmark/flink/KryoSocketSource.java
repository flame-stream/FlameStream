package com.spbsu.benchmark.flink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.spbsu.flamestream.example.bl.index.model.WikipediaPage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.jetbrains.annotations.Nullable;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KryoSocketSource extends RichParallelSourceFunction<WikipediaPage> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(KryoSocketSource.class);
  public static final int INPUT_BUFFER_SIZE = 20_000_000;
  public static final int CONNECTION_AWAIT_TIMEOUT = 5000;

  private final String hostname;
  private final int port;

  @Nullable
  private transient Client client = null;

  public KryoSocketSource(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public void open(Configuration parameters) {
    client = new Client(1000, INPUT_BUFFER_SIZE);
    client.getKryo().register(WikipediaPage.class);
    ((Kryo.DefaultInstantiatorStrategy) client.getKryo()
            .getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  @Override
  public void run(SourceContext<WikipediaPage> ctx) throws Exception {
    client.addListener(new Listener() {
      @Override
      public void received(Connection connection, Object object) {
        if (object instanceof WikipediaPage) {
          ctx.collect((WikipediaPage) object);
        } else {
          LOG.warn("WTF: {}", object);
        }
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
    client.connect(CONNECTION_AWAIT_TIMEOUT, hostname, port);
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
