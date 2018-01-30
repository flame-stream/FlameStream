package com.spbsu.benchmark.flink.sum;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.jetbrains.annotations.Nullable;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SumSockerSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(SumSockerSource.class);
  private static final int INPUT_BUFFER_SIZE = 20_000_000;
  private static final int CONNECTION_AWAIT_TIMEOUT = 5000;

  private final String hostname;
  private final int port;

  @Nullable
  private transient Client client = null;
  private long prevGlobalTs = 0;

  public SumSockerSource(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public void open(Configuration parameters) {
    client = new Client(1000, INPUT_BUFFER_SIZE);
    ((Kryo.DefaultInstantiatorStrategy) client.getKryo()
            .getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
    client.getKryo().register(Tuple2.class);
  }

  @Override
  public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
    client.addListener(new Listener() {
      @Override
      public void received(Connection connection, Object object) {
        if (object instanceof Tuple2) {
          final Tuple2<Long, Long> t = ((Tuple2<Long, Long>) object);
          ctx.collectWithTimestamp(t, t.f0);
          //if (t.f0 % 100 == 0) {
          ctx.emitWatermark(new Watermark(t.f0));
          //}
        } else {
          LOG.warn("WTF: {}", object);
        }
      }
    });

    client.addListener(new Listener() {
      @Override
      public void disconnected(Connection connection) {
        LOG.info("DISCONNECTED");
        ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
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
