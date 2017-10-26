package com.spbsu.benchmark.flink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexAdd;
import com.spbsu.flamestream.example.inverted_index.model.WordIndexRemove;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.jetbrains.annotations.Nullable;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KryoSocketSink extends RichSinkFunction<InvertedIndexStream.Result> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(KryoSocketSink.class);

  private final String hostName;
  private final int port;

  @Nullable
  private transient Client client;

  public KryoSocketSink(String hostName, int port) {
    this.hostName = hostName;
    this.port = port;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    client = new Client(20_000_000, 1234);
    client.getKryo().register(InvertedIndexStream.Result.class);
    client.getKryo().register(WordIndexAdd.class);
    client.getKryo().register(WordIndexRemove.class);
    client.getKryo().register(long[].class);
    ((Kryo.DefaultInstantiatorStrategy) client.getKryo().getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());

    client.addListener(new Listener() {
      @Override
      public void disconnected(Connection connection) {
        System.out.println("Sink has been disconnected " + connection);
      }
    });

    LOG.info("Connecting to {}:{}", hostName, port);
    client.start();
    client.connect(5000, hostName, port);
    LOG.info("Connected to {}:{}", hostName, port);
  }

  @Override
  public void invoke(InvertedIndexStream.Result value) throws Exception {
    if (client != null && client.isConnected()) {
      client.sendTCP(value);
    } else {
      LOG.warn("Writing to closed log");
    }
  }

  @Override
  public void close() throws Exception {
    if (client != null) {
      LOG.info("Closing sink connection");
      client.close();
      client.stop();
    }
  }
}

