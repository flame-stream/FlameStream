package com.spbsu.experiments.inverted_index;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.HashRange;
import com.spbsu.datastream.core.application.WorkerApplication;
import com.spbsu.datastream.core.application.ZooKeeperApplication;
import com.spbsu.datastream.core.deploy.DeployApi;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.graph.ops.FlatFilter;
import com.spbsu.datastream.core.graph.ops.SpliteratorSource;
import com.spbsu.datastream.core.node.MyPaths;
import com.spbsu.datastream.core.test.InitZookeeper;
import com.spbsu.experiments.inverted_index.common_bl.actions.PageToWordPositionsFilter;
import com.spbsu.experiments.inverted_index.common_bl.io.WikiPageIterator;
import com.spbsu.experiments.inverted_index.common_bl.models.WikiPage;
import com.spbsu.experiments.inverted_index.common_bl.models.WordContainer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class RunZKWorkersDeployer {
  public static void main(final String... args) throws Exception {
    new RunZKWorkersDeployer().run();
  }

  public void run() throws Exception {
    runAgents();
    TimeUnit.SECONDS.sleep(5);
    deploy();
  }

  public void runAgents() throws Exception {
    ZooKeeperApplication.main();
    TimeUnit.SECONDS.sleep(5);

    InitZookeeper.main();
    TimeUnit.SECONDS.sleep(5);


    final InetSocketAddress w1 = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7001);
    final InetSocketAddress w2 = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7002);

    new WorkerApplication().run(w1, "localhost:2181");
    new WorkerApplication().run(w2, "localhost:2181");
  }

  public void deploy() {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + 12341)
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLoopbackAddress().getHostName()))
            .withFallback(ConfigFactory.load("remote"));
    final ActorSystem system = ActorSystem.create("requester", config);


    // TODO: 3/26/17
    final TheGraph theGraph = null;
    final long tick = (int) (System.currentTimeMillis() / TimeUnit.MINUTES.toMillis(13));
    final DeployApi.DeployForTick request = new DeployApi.DeployForTick(theGraph, tick);

    final ActorSelection worker1 = rangeConcierge(system, 7001, new HashRange(Integer.MIN_VALUE, 0));
    final ActorSelection worker2 = rangeConcierge(system, 7002, new HashRange(0, Integer.MAX_VALUE));
    worker1.tell(request, null);
    worker2.tell(request, null);
  }

  private ActorSelection rangeConcierge(final ActorSystem system,
                                        final int port,
                                        final HashRange range) {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    final ActorPath rangeConcierge = MyPaths.rangeConcierge(address, range);
    return system.actorSelection(rangeConcierge);
  }

  private TheGraph theGraph() {
    final Spliterator<WikiPage> spliterator = new LazySpliterator<>(new SplSupplier());
    final SpliteratorSource<WikiPage> source = new SpliteratorSource<>(spliterator);

    final FlatFilter<WordContainer, WordContainer> flatter = new FlatFilter<>(new PageToWordPositionsFilter());

    final WordGrouping wordGrouping = new WordGrouping();
    return null;
  }

  private static class SplSupplier implements Supplier<Spliterator<WikiPage>> {
    @Override
    public Spliterator<WikiPage> get() {
      final ClassLoader classLoader = getClass().getClassLoader();
      final InputStream stream = classLoader.getResourceAsStream("wikipedia/small_dump_example.xml");
      return Spliterators.spliterator(new WikiPageIterator(stream), Long.MAX_VALUE, 0);
    }
  }
}
