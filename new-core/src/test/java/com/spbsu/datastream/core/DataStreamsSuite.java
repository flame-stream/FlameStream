package com.spbsu.datastream.core;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.application.WorkerApplication;
import com.spbsu.datastream.core.application.ZooKeeperApplication;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.front.RawData;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.node.DeployForTick;
import com.spbsu.datastream.core.node.MyPaths;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.zookeeper.KeeperException;
import org.jooq.lambda.Unchecked;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class DataStreamsSuite {
  private static final int START_WORKER_PORT = 5223;
  private static final int START_FRONT_PORT = 5323;

  //Suite data
  private final Map<HashRange, InetSocketAddress> workers = DataStreamsSuite.workers(10);
  private final Map<Integer, InetSocketAddress> fronts = DataStreamsSuite.fronts(this.workers);

  //Test method data
  private final Collection<WorkerApplication> workerApplication = new HashSet<>();
  private ActorSystem localSystem;

  @BeforeSuite
  public final void beforeSuite() throws IOException, InterruptedException {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + 12341)
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLoopbackAddress().getHostName()))
            .withFallback(ConfigFactory.load("remote"));
    this.localSystem = ActorSystem.create("requester", config);

    new Thread(Unchecked.runnable(() -> new ZooKeeperApplication().run())).start();
    TimeUnit.SECONDS.sleep(2);
  }

  @AfterSuite
  public final void afterSuite() throws IOException, InterruptedException {
    // TODO: 5/2/17 Graceful stop
    this.localSystem.shutdown();

    // TODO: 5/2/17 kill ZK
    Files.newDirectoryStream(Paths.get("zookeeper", "version-2")).forEach(Unchecked.consumer(Files::delete));
  }

  @BeforeMethod
  public final void prepareEnvironment() throws IOException, KeeperException, InterruptedException {
    try (final ZK zkConfigurationDeployer = new ZK("localhost:2181")) {
      zkConfigurationDeployer.pushFrontMappings(this.fronts);
      zkConfigurationDeployer.pushRangeMappings(this.workers);
    }

    for (final InetSocketAddress workerAddress : this.workers.values()) {
      final WorkerApplication worker = new WorkerApplication(workerAddress, "localhost:2181");
      final Thread workerThread = new Thread(worker::run);

      this.workerApplication.add(worker);
      workerThread.start();
    }

    for (final InetSocketAddress front : this.fronts.values()) {
      final WorkerApplication worker = new WorkerApplication(front, "localhost:2181");
      final Thread workerThread = new Thread(worker::run);

      this.workerApplication.add(worker);
      workerThread.start();
    }

    TimeUnit.SECONDS.sleep(5);
  }

  @AfterMethod
  public final void clearEnvironment() {
    this.workerApplication.forEach(WorkerApplication::shutdown);
    this.workerApplication.clear();
  }

  protected final void deploy(final TheGraph theGraph) {
    final DeployForTick deployForTick = new DeployForTick(
            theGraph,
            this.workers.keySet().stream().findAny().orElseThrow(RuntimeException::new),
            System.nanoTime(),
            TimeUnit.MILLISECONDS.toNanos(100)
    );

    Stream.concat(this.workers.values().stream(), this.fronts.values().stream())
            .map(MyPaths::nodeConcierge)
            .map(path -> this.localSystem.actorSelection(path))
            .forEach(con -> con.tell(deployForTick, ActorRef.noSender()));
  }

  protected final Set<Integer> fronts() {
    return this.fronts.keySet();
  }

  protected final Consumer<Object> randomConsumer() {
    final List<Consumer<Object>> result = new ArrayList<>();

    for (final Map.Entry<Integer, InetSocketAddress> front : this.fronts.entrySet()) {
      final ActorSelection frontActor = DataStreamsSuite.front(this.localSystem, front.getValue());
      final Consumer<Object> consumer = obj -> frontActor.tell(new RawData<>(obj), ActorRef.noSender());
      result.add(consumer);
    }

    final Random rd = new Random();

    return obj -> result.get(rd.nextInt(result.size())).accept(obj);
  }

  protected final <T> Consumer<T> wrap(final Queue<T> collection) {
    final ActorRef consumerActor = this.localSystem.actorOf(CollectorActor.props(collection));
    return new ActorConsumer<>(consumerActor);
  }

  private static final class ActorConsumer<T> implements Consumer<T> {
    private final ActorRef actorRef;

    ActorConsumer(final ActorRef actorRef) {
      this.actorRef = actorRef;
    }

    @Override
    public void accept(final T o) {
      this.actorRef.tell(o, ActorRef.noSender());
    }
  }

  private static Map<Integer, InetSocketAddress> fronts(
          final Map<HashRange, InetSocketAddress> correspondingWorkers) {
    try {
      final Map<Integer, InetSocketAddress> fronts = new HashMap<>();

      int port = DataStreamsSuite.START_FRONT_PORT;

      for (final HashRange range : correspondingWorkers.keySet()) {
        fronts.put(range.from(), new InetSocketAddress(InetAddress.getLocalHost(), port));
        port += 1;
      }

      return fronts;
    } catch (final UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("NumericCastThatLosesPrecision")
  private static Map<HashRange, InetSocketAddress> workers(final int count) {
    try {
      final int step = (int) (((long) Integer.MAX_VALUE - Integer.MIN_VALUE) / count);

      final Map<HashRange, InetSocketAddress> workers = new HashMap<>();

      int port = DataStreamsSuite.START_WORKER_PORT;
      long left = Integer.MIN_VALUE;
      long right = left + step;

      for (int i = 0; i < count; ++i) {
        workers.put(new HashRange((int) left, (int) right), new InetSocketAddress(InetAddress.getLocalHost(), port));

        left += step;
        right = Math.min(Integer.MAX_VALUE, right + step);
        port += 1;
      }

      return workers;
    } catch (final UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("TypeMayBeWeakened")
  private static ActorSelection nodeConcierge(final ActorSystem system,
                                              final int port) {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    final ActorPath rangeConcierge = MyPaths.nodeConcierge(address);
    return system.actorSelection(rangeConcierge);
  }

  @SuppressWarnings("TypeMayBeWeakened")
  private static ActorSelection front(final ActorSystem system,
                                      final InetSocketAddress address) {
    final ActorPath front = MyPaths.front(address);
    return system.actorSelection(front);
  }
}
