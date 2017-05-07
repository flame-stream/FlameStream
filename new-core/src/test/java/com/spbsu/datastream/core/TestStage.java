package com.spbsu.datastream.core;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.spbsu.datastream.core.application.WorkerApplication;
import com.spbsu.datastream.core.application.ZooKeeperApplication;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.front.RawData;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.node.MyPaths;
import com.spbsu.datastream.core.node.TickInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jooq.lambda.Unchecked;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("ProhibitedExceptionThrown")
public final class TestStage implements Closeable {
  private static final String ZK_STRING = "localhost:2181";
  private static final int LOCAL_SYSTEM_PORT = 12345;

  private static final int START_WORKER_PORT = 5223;

  private final Map<Integer, InetSocketAddress> dns;
  private final Map<HashRange, Integer> workers;
  private final Set<Integer> fronts;

  //Test method data
  private final Collection<WorkerApplication> workerApplication = new HashSet<>();
  private final ActorSystem localSystem;

  public TestStage(final int workersCount) {
    this.dns = TestStage.dns(workersCount);
    this.workers = TestStage.workers(this.dns.keySet());
    this.fronts = this.dns.keySet();

    try {
      final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + TestStage.LOCAL_SYSTEM_PORT)
              .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLocalHost().getHostName()))
              .withFallback(ConfigFactory.load("remote"));
      this.localSystem = ActorSystem.create("requester", config);

      Files.newDirectoryStream(Paths.get("zookeeper/version-2")).forEach(Unchecked.consumer(Files::delete));
      new Thread(Unchecked.runnable(() -> new ZooKeeperApplication().run())).start();

      TimeUnit.SECONDS.sleep(5);
      this.deployPartitioning();
      this.workerApplication.addAll(this.startWorkers());
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<Integer, InetSocketAddress> dns(final int workersCount) {
    return IntStream.range(TestStage.START_WORKER_PORT, TestStage.START_WORKER_PORT + workersCount)
            .boxed().collect(Collectors.toMap(Function.identity(),
                    Unchecked.function(port -> new InetSocketAddress(InetAddress.getLocalHost(), port))));
  }

  @SuppressWarnings("NumericCastThatLosesPrecision")
  private static Map<HashRange, Integer> workers(final Collection<Integer> workers) {
    final Map<HashRange, Integer> result = new HashMap<>();

    final int step = (int) (((long) Integer.MAX_VALUE - Integer.MIN_VALUE) / workers.size());
    long left = Integer.MIN_VALUE;
    long right = left + step;

    for (final int workerId : workers) {
      result.put(new HashRange((int) left, (int) right), workerId);

      left += step;
      right = Math.min(Integer.MAX_VALUE, right + step);
    }

    return result;
  }

  private void deployPartitioning() throws Exception {
    try (final ZKDeployer zkConfigurationDeployer = new ZKDeployer(TestStage.ZK_STRING)) {
      zkConfigurationDeployer.createDirs();
      zkConfigurationDeployer.pushDNS(this.dns);
      zkConfigurationDeployer.pushFrontMappings(this.fronts);
      zkConfigurationDeployer.pushRangeMappings(this.workers);
    }
  }

  private Collection<WorkerApplication> startWorkers() {
    final Set<WorkerApplication> apps = this.dns.entrySet().stream()
            .map(f -> new WorkerApplication(f.getKey(), f.getValue(), TestStage.ZK_STRING))
            .collect(Collectors.toSet());

    apps.forEach(WorkerApplication::run);
    return apps;
  }

  public Set<Integer> fronts() {
    return Collections.unmodifiableSet(this.fronts);
  }

  @Override
  public void close() {
    try {
      Files.newDirectoryStream(Paths.get("zookeeper/version-2")).forEach(Unchecked.consumer(Files::delete));
      // TODO: 5/2/17 Graceful stop
      this.localSystem.shutdown();

      this.workerApplication.forEach(WorkerApplication::shutdown);
      this.workerApplication.clear();
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  public <T> ActorPath wrap(final Queue<T> collection) {
    try {
      final String id = UUID.randomUUID().toString();
      final ActorRef consumerActor = this.localSystem.actorOf(CollectorActor.props(collection), id);
      final Address add = Address.apply("akka.tcp", "requester",
              InetAddress.getLocalHost().getHostName(),
              TestStage.LOCAL_SYSTEM_PORT);
      return RootActorPath.apply(add, "/")
              .$div("user")
              .$div(id);
    } catch (final UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  public void deploy(final TheGraph theGraph) {
    final long startTs = System.nanoTime();
    final TickInfo tickInfo = new TickInfo(
            theGraph,
            this.workers.keySet().stream().findAny().orElseThrow(RuntimeException::new),
            startTs,
            startTs + TimeUnit.SECONDS.toNanos(30),
            TimeUnit.MILLISECONDS.toNanos(100)
    );
    try (final ZKDeployer zkConfigurationDeployer = new ZKDeployer(TestStage.ZK_STRING)) {
      zkConfigurationDeployer.pushTick(tickInfo);
      TimeUnit.SECONDS.sleep(2);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Consumer<Object> randomFrontConsumer() {
    final List<Consumer<Object>> result = new ArrayList<>();

    for (final int frontId : this.fronts) {
      final ActorSelection frontActor = TestStage.front(this.localSystem, this.dns.get(frontId));
      final Consumer<Object> consumer = obj -> frontActor.tell(new RawData<>(obj), ActorRef.noSender());
      result.add(consumer);
    }

    final Random rd = new Random();
    return obj -> result.get(rd.nextInt(result.size())).accept(obj);
  }

  @SuppressWarnings("TypeMayBeWeakened")
  private static ActorSelection front(final ActorSystem system,
                                      final InetSocketAddress address) {
    final ActorPath front = MyPaths.front(address);
    return system.actorSelection(front);
  }
}
