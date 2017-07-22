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
import com.spbsu.datastream.core.tick.TickInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.jooq.lambda.Unchecked;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class TestStand implements Closeable {
  private static final String ZK_STRING = "localhost:2181";
  private static final int LOCAL_SYSTEM_PORT = 12345;

  private static final int START_WORKER_PORT = 5223;

  private final Map<Integer, InetSocketAddress> dns;
  private final Set<Integer> fronts;

  private final Collection<WorkerApplication> workerApplication = new HashSet<>();

  private final ZooKeeperApplication zk;
  private final Thread zkThread;
  private final ActorSystem localSystem;

  public TestStand(int workersCount, int frontCount) {
    this.dns = TestStand.dns(workersCount);
    this.fronts = this.dns.keySet().stream().limit(frontCount).collect(Collectors.toSet());

    try {
      final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + TestStand.LOCAL_SYSTEM_PORT)
              .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLocalHost().getHostName()))
              .withFallback(ConfigFactory.load("remote"));

      FileUtils.deleteDirectory(new File("zookeeper"));
      FileUtils.deleteDirectory(new File("leveldb"));

      this.localSystem = ActorSystem.create("requester", config);
      this.zk = new ZooKeeperApplication();
      this.zkThread = new Thread(Unchecked.runnable(this.zk::run));
      this.zkThread.start();

      TimeUnit.SECONDS.sleep(1);
      this.deployPartitioning();

      this.workerApplication.addAll(this.startWorkers());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      Await.ready(this.localSystem.terminate(), Duration.Inf());
      this.workerApplication.forEach(WorkerApplication::shutdown);
      this.workerApplication.clear();

      this.zk.shutdown();
      this.zkThread.join();

      FileUtils.deleteDirectory(new File("zookeeper"));
      FileUtils.deleteDirectory(new File("leveldb"));
    } catch (InterruptedException | TimeoutException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Collection<Integer> fronts() {
    return Collections.unmodifiableSet(this.fronts);
  }

  public <T> ActorPath wrap(Consumer<T> collection) {
    try {
      final String id = UUID.randomUUID().toString();
      final ActorRef consumerActor = this.localSystem.actorOf(CollectingActor.props(collection), id);
      final Address add = Address.apply("akka.tcp", "requester",
              InetAddress.getLocalHost().getHostName(),
              TestStand.LOCAL_SYSTEM_PORT);
      return RootActorPath.apply(add, "/")
              .$div("user")
              .$div(id);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  public void deploy(TheGraph theGraph, int tickLength, TimeUnit timeUnit) {
    final long startTs = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);

    final Map<HashRange, Integer> workers = TestStand.workers(this.dns.keySet());

    final TickInfo tickInfo = new TickInfo(
            theGraph,
            workers.values().stream().findAny().orElseThrow(RuntimeException::new),
            workers,
            startTs,
            startTs + timeUnit.toNanos(tickLength),
            TimeUnit.MILLISECONDS.toNanos(10)
    );
    try (final ZKDeployer zkConfigurationDeployer = new ZKDeployer(TestStand.ZK_STRING)) {
      zkConfigurationDeployer.pushTick(tickInfo);
      TimeUnit.SECONDS.sleep(2);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void waitTick(int tickLength, TimeUnit unit) throws InterruptedException {
    unit.sleep(tickLength);
  }

  public Consumer<Object> randomFrontConsumer(long seed) {
    final List<Consumer<Object>> result = new ArrayList<>();

    for (int frontId : this.fronts) {
      final ActorSelection frontActor = TestStand.front(this.localSystem, frontId, this.dns.get(frontId));
      final Consumer<Object> consumer = obj -> frontActor.tell(new RawData<>(obj), ActorRef.noSender());
      result.add(consumer);
    }

    final Random rd = new Random(seed);
    return obj -> result.get(rd.nextInt(result.size())).accept(obj);
  }

  private static Map<Integer, InetSocketAddress> dns(int workersCount) {
    return IntStream.range(TestStand.START_WORKER_PORT, TestStand.START_WORKER_PORT + workersCount)
            .boxed().collect(Collectors.toMap(Function.identity(),
                    Unchecked.function(port -> new InetSocketAddress(InetAddress.getLocalHost(), port))));
  }

  @SuppressWarnings("NumericCastThatLosesPrecision")
  private static Map<HashRange, Integer> workers(Collection<Integer> workers) {
    final Map<HashRange, Integer> result = new HashMap<>();

    final int step = (int) (((long) Integer.MAX_VALUE - Integer.MIN_VALUE) / workers.size());
    long left = Integer.MIN_VALUE;
    long right = left + step;

    for (int workerId : workers) {
      result.put(new HashRange((int) left, (int) right), workerId);

      left += step;
      right = Math.min(Integer.MAX_VALUE, right + step);
    }

    return result;
  }

  @SuppressWarnings("TypeMayBeWeakened")
  private static ActorSelection front(ActorSystem system,
                                      int id,
                                      InetSocketAddress address) {
    final Address add = Address.apply("akka.tcp", "worker", address.getAddress().getHostName(), address.getPort());
    final ActorPath path = RootActorPath.apply(add, "/")
            .$div("user")
            .$div("watcher")
            .$div(String.valueOf(id))
            .$div("front");
    return system.actorSelection(path);
  }

  private void deployPartitioning() throws Exception {
    try (final ZKDeployer zkConfigurationDeployer = new ZKDeployer(TestStand.ZK_STRING)) {
      zkConfigurationDeployer.createDirs();
      zkConfigurationDeployer.pushDNS(this.dns);
      zkConfigurationDeployer.pushFronts(this.fronts);
    }
  }

  private Collection<WorkerApplication> startWorkers() {
    final Set<WorkerApplication> apps = this.dns.entrySet().stream()
            .map(f -> new WorkerApplication(f.getKey(), f.getValue(), TestStand.ZK_STRING))
            .collect(Collectors.toSet());

    apps.forEach(WorkerApplication::run);
    return apps;
  }
}
