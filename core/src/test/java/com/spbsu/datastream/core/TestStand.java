package com.spbsu.datastream.core;

import akka.actor.*;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.graph.TheGraph;
import com.spbsu.datastream.core.raw.SingleRawData;
import com.spbsu.datastream.core.tick.TickInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;

public final class TestStand implements AutoCloseable {
  private static final int LOCAL_SYSTEM_PORT = 12345;

  private final ActorSystem localSystem;

  private final Cluster cluster;

  public TestStand(Cluster cluster) {
    this.cluster = cluster;

    try {
      final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + TestStand.LOCAL_SYSTEM_PORT)
              .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLocalHost().getHostName()))
              .withFallback(ConfigFactory.load("remote"));

      this.localSystem = ActorSystem.create("requester", config);
    } catch (UnknownHostException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      Await.ready(localSystem.terminate(), Duration.Inf());
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  public Collection<Integer> frontIds() {
    return unmodifiableSet(cluster.fronts());
  }


  /**
   * Wraps collection with an Actor.
   * <p>
   * NB: Collection must be properly synchronized
   */
  public <T> ActorPath wrap(Consumer<T> collection) {
    try {
      final String id = UUID.randomUUID().toString();
      localSystem.actorOf(CollectingActor.props(collection), id);
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

    final Map<HashRange, Integer> workers = rangeMappingForTick();

    final TickInfo tickInfo = new TickInfo(
            theGraph,
            workers.values().stream().findAny().orElseThrow(RuntimeException::new),
            workers,
            startTs,
            startTs + timeUnit.toNanos(tickLength),
            TimeUnit.MILLISECONDS.toNanos(10)
    );

    try (final ZookeeperDeployer zkDeployer = new ZookeeperDeployer(cluster.zookeeperString())) {
      zkDeployer.pushTick(tickInfo);
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

    final Set<InetSocketAddress> frontAddresses = cluster.fronts().stream()
            .map(id -> cluster.nodes().get(id)).collect(Collectors.toSet());

    for (InetSocketAddress front : frontAddresses) {
      final ActorSelection frontActor = frontActor(front);
      final Consumer<Object> consumer = obj -> frontActor.tell(new SingleRawData<>(obj), ActorRef.noSender());
      result.add(consumer);
    }

    final Random rd = new Random(seed);
    return obj -> result.get(rd.nextInt(result.size())).accept(obj);
  }

  @SuppressWarnings("NumericCastThatLosesPrecision")
  private Map<HashRange, Integer> rangeMappingForTick() {
    final Map<HashRange, Integer> result = new HashMap<>();
    final Set<Integer> workerIds = cluster.nodes().keySet();

    final int step = (int) (((long) Integer.MAX_VALUE - Integer.MIN_VALUE) / workerIds.size());
    long left = Integer.MIN_VALUE;
    long right = left + step;

    for (int workerId : workerIds) {
      result.put(new HashRange((int) left, (int) right), workerId);

      left += step;
      right = Math.min(Integer.MAX_VALUE, right + step);
    }

    return result;
  }

  private ActorSelection frontActor(InetSocketAddress address) {
    final Address add = Address.apply("akka.tcp", "worker", address.getAddress().getHostName(), address.getPort());
    final ActorPath path = RootActorPath.apply(add, "/")
            .$div("user")
            .$div("watcher")
            .$div("concierge")
            .$div("front");
    return localSystem.actorSelection(path);
  }
}
