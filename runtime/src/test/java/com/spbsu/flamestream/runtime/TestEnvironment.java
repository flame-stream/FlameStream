package com.spbsu.flamestream.runtime;

import akka.actor.ActorIdentity;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.RootActorPath;
import akka.japi.pf.ReceiveBuilder;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.atomic.AtomicGraph;
import com.spbsu.flamestream.core.graph.composed.ComposedGraph;
import com.spbsu.flamestream.runtime.actor.LoggingActor;
import com.spbsu.flamestream.runtime.environment.Environment;
import com.spbsu.flamestream.runtime.front.ActorFront;
import com.spbsu.flamestream.runtime.range.HashRange;
import com.spbsu.flamestream.runtime.raw.RawData;
import com.spbsu.flamestream.runtime.raw.SingleRawData;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public class TestEnvironment implements Environment {
  private static final long DEFAULT_TEST_WINDOW = 10;

  private final DumbInetSocketAddress actorSysAddress;
  private final ActorSystem system;
  private final Environment innerEnvironment;
  private final long windowInMillis;

  public TestEnvironment(Environment inner) {
    this(inner, DEFAULT_TEST_WINDOW);
  }

  public TestEnvironment(Environment inner, long windowInMillis) {
    this.innerEnvironment = inner;
    this.windowInMillis = windowInMillis;

    try {
      actorSysAddress = new DumbInetSocketAddress(InetAddress.getLocalHost().getHostName(), 23456);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }

    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + actorSysAddress.port())
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + actorSysAddress.host()))
            .withFallback(ConfigFactory.load("remote"));
    this.system = ActorSystem.create("environment", config);
  }

  // TODO: 13.11.2017 accept graph instead of composed graph
  public Consumer<Object> deploy(Graph graph,
          int tickLengthSeconds,
          int ticksCount,
          int frontsCount) {
    final Consumer<Object> consumer;
    { //create consumer
      final ActorRef balancingActor = system.actorOf(Props.create(BalancingActor.class), "balancing-actor");
      final Address address = Address.apply("akka.tcp", "environment", actorSysAddress.host(), actorSysAddress.port());
      final ActorPath path = RootActorPath.apply(address, "/").child("user").child("balancing-actor");
      final List<Props> props = IntStream.range(0, frontsCount)
              .mapToObj(String::valueOf)
              .map(id -> ActorFront.props(id, path))
              .collect(Collectors.toList());

      final List<String> workers = new ArrayList<>(availableWorkers());
      for (int i = 0; i < props.size(); i++) {
        deployFront(workers.get(i % workers.size()), "front-" + i, props.get(i));
      }
      consumer = o -> balancingActor.tell(new SingleRawData<>(o), ActorRef.noSender());
    }

    final Map<HashRange, String> workers = rangeMappingForTick();
    final long tickMills = SECONDS.toMillis(tickLengthSeconds);
    long startTs = System.currentTimeMillis();
    for (int i = 0; i < ticksCount; ++i, startTs += tickMills) {
      //noinspection ConstantConditions
      final TickInfo tickInfo = new TickInfo(
              i,
              startTs,
              startTs + tickMills,
              graph.flattened(),
              workers.values().stream().min(String::compareTo).get(),
              workers,
              IntStream.range(0, frontsCount).boxed().map(String::valueOf).collect(Collectors.toSet()),
              windowInMillis,
              i == 0 ? emptySet() : singleton(i - 1L)
      );
      innerEnvironment.deploy(tickInfo);
    }

    //This sleep doesn't affect correctness.
    // Only reduces stashing overhead for first several items
    try {
      SECONDS.sleep(2);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return consumer;
  }

  private Map<HashRange, String> rangeMappingForTick() {
    final Map<HashRange, String> result = new HashMap<>();
    final Set<String> workerIds = innerEnvironment.availableWorkers();

    final int step = (int) (((long) Integer.MAX_VALUE - Integer.MIN_VALUE) / workerIds.size());
    long left = Integer.MIN_VALUE;
    long right = left + step;

    for (String workerId : workerIds) {
      result.put(new HashRange((int) left, (int) right), workerId);

      left += step;
      right = Math.min(Integer.MAX_VALUE, right + step);
    }

    return result;
  }

  public void awaitTicks() throws InterruptedException {
    for (long tick : ticks()) {
      awaitTick(tick);
    }
  }

  @Override
  public void close() {
    try {
      innerEnvironment.close();
      Await.ready(system.terminate(), Duration.Inf());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deploy(TickInfo tickInfo) {
    innerEnvironment.deploy(tickInfo);
  }

  @Override
  public void deployFront(String nodeId, String frontId, Props frontProps) {
    innerEnvironment.deployFront(nodeId, frontId, frontProps);
  }

  @Override
  public Set<String> availableWorkers() {
    return innerEnvironment.availableWorkers();
  }

  @Override
  public <T> AtomicGraph wrapInSink(ToIntFunction<? super T> hash, Consumer<? super T> mySuperConsumer) {
    return innerEnvironment.wrapInSink(hash, mySuperConsumer);
  }

  @Override
  public void awaitTick(long tickId) throws InterruptedException {
    innerEnvironment.awaitTick(tickId);
  }

  @Override
  public Set<Long> ticks() {
    return innerEnvironment.ticks();
  }

  private static class BalancingActor extends LoggingActor {
    private final List<ActorRef> fronts = new ArrayList<>();

    @Override
    public Receive createReceive() {
      return ReceiveBuilder.create()
              .match(ActorIdentity.class, i -> {
                fronts.add(sender());
                unstashAll();

                getContext().become(ReceiveBuilder.create()
                        .match(ActorIdentity.class, id -> fronts.add(sender()))
                        .match(
                                RawData.class,
                                r -> fronts.get(ThreadLocalRandom.current().nextInt(fronts.size())).tell(r, self())
                        )
                        .build()
                );
              })
              .matchAny(o -> stash())
              .build();
    }
  }
}
