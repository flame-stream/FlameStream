package com.spbsu.benchmark.flink;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import com.spbsu.flamestream.example.inverted_index.model.WikipediaPage;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class MySocketSource extends RichSourceFunction<WikipediaPage> {

  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(MySocketSource.class);

  private final String hostname;
  private final int port;

  private transient Client currentSocket;

  public MySocketSource(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    currentSocket = new Client();
    currentSocket.connect(5000, InetAddress.getByName(hostname), port);

    currentSocket.getKryo().register(WikipediaPage.class);
    ((Kryo.DefaultInstantiatorStrategy) currentSocket.getKryo().getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  @Override
  public void run(SourceContext<WikipediaPage> ctx) throws Exception {
    currentSocket.addListener(new Listener() {
      public void received(Connection connection, Object object) {
        if (object instanceof WikipediaPage) {
          ctx.collect((WikipediaPage) object);
        }
      }
    });
  }

  @Override
  public void cancel() {
    final Client theSocket = currentSocket;
    if (theSocket != null) {
      theSocket.close();
    }
  }
}
