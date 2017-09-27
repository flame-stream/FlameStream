package com.spbsu.flamestream.runtime.environmet.local;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.spbsu.flamestream.core.graph.TheGraph;
import com.spbsu.flamestream.core.data.raw.SingleRawData;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class TestStand implements AutoCloseable {


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

  public void deploy(TheGraph theGraph, int tickLengthSeconds, int ticksCount) {
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
