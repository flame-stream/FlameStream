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

import java.io.Closeable;
import java.io.File;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("ProhibitedExceptionThrown")
final class TestStand implements Closeable {
  private static final String ZK_STRING = "localhost:2181";
  private static final int LOCAL_SYSTEM_PORT = 12345;

  private static final int START_WORKER_PORT = 5223;

  private final Map<Integer, InetSocketAddress> dns;
  private final Map<HashRange, Integer> workers;
  private final Set<Integer> fronts;

  private final Collection<WorkerApplication> workerApplication = new HashSet<>();

  private final ZooKeeperApplication zk;
  private final Thread zkThread;
  private final ActorSystem localSystem;

  TestStand(int workersCount) {
    this.dns = TestStand.dns(workersCount);
    this.workers = TestStand.workers(this.dns.keySet());
    this.fronts = this.dns.keySet();

    try {
      final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + TestStand.LOCAL_SYSTEM_PORT)
              .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLocalHost().getHostName()))
              .withFallback(ConfigFactory.load("remote"));
      this.localSystem = ActorSystem.create("requester", config);

      FileUtils.deleteDirectory(new File("zookeeper"));
      FileUtils.deleteDirectory(new File("leveldb"));
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
      this.zk.shutdown();
      this.zkThread.join();

      // TODO: 5/18/17  
      //this.localSystem.shutdown();

      this.workerApplication.forEach(WorkerApplication::shutdown);
      this.workerApplication.clear();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Collection<Integer> fronts() {
    return Collections.unmodifiableSet(this.fronts);
  }

  public <T> ActorPath wrap(Queue<T> collection) {
    try {
      final String id = UUID.randomUUID().toString();
      final ActorRef consumerActor = this.localSystem.actorOf(CollectorActor.props(collection), id);
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

  public void deploy(TheGraph theGraph) {
    final long startTs = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
    final TickInfo tickInfo = new TickInfo(
            theGraph,
            this.workers.values().stream().findAny().orElseThrow(RuntimeException::new),
            this.workers,
            startTs,
            startTs + TimeUnit.SECONDS.toNanos(10),
            TimeUnit.MILLISECONDS.toNanos(10)
    );
    try (final ZKDeployer zkConfigurationDeployer = new ZKDeployer(TestStand.ZK_STRING)) {
      zkConfigurationDeployer.pushTick(tickInfo);
      TimeUnit.SECONDS.sleep(1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void waitTick() throws InterruptedException {
    TimeUnit.SECONDS.sleep(10);
  }

  public Consumer<Object> randomFrontConsumer() {
    final List<Consumer<Object>> result = new ArrayList<>();

    for (int frontId : this.fronts) {
      final ActorSelection frontActor = TestStand.front(this.localSystem, frontId, this.dns.get(frontId));
      final Consumer<Object> consumer = obj -> frontActor.tell(new RawData<>(obj), ActorRef.noSender());
      result.add(consumer);
    }

    final Random rd = new Random();
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
            .$div(String.valueOf(id))
            .$div("front");
    return system.actorSelection(path);
  }

  private void deployPartitioning() throws Exception {
    try (final ZKDeployer zkConfigurationDeployer = new ZKDeployer(TestStand.ZK_STRING)) {
      zkConfigurationDeployer.createDirs();
      zkConfigurationDeployer.pushDNS(this.dns);
      zkConfigurationDeployer.pushFrontMappings(this.fronts);
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
