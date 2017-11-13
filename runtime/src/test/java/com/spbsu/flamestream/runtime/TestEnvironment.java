package com.spbsu.flamestream.runtime;

import akka.actor.Props;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.ComposedGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.InPort;
import com.spbsu.flamestream.runtime.environment.Environment;
import com.spbsu.flamestream.runtime.range.HashRange;
import com.spbsu.flamestream.runtime.tick.TickInfo;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * User: Artem
 * Date: 28.09.2017
 */
public class TestEnvironment implements Environment {
  private static final long DEFAULT_TEST_WINDOW = 10;

  private final Environment innerEnvironment;
  private final long windowInMillis;
  private final Map<Integer, Consumer<Object>> consumers = new HashMap<>();

  public TestEnvironment(Environment inner) {
    this(inner, DEFAULT_TEST_WINDOW);
  }

  public TestEnvironment(Environment inner, long windowInMillis) {
    this.innerEnvironment = inner;
    this.windowInMillis = windowInMillis;
  }

  public void deploy(ComposedGraph<AtomicGraph> graph, int tickLengthSeconds, int ticksCount) {
    final Map<HashRange, Integer> workers = rangeMappingForTick();
    final long tickMills = SECONDS.toMillis(tickLengthSeconds);

    long startTs = System.currentTimeMillis();
    for (int i = 0; i < ticksCount; ++i, startTs += tickMills) {
      //noinspection ConstantConditions
      final TickInfo tickInfo = new TickInfo(
              i,
              startTs,
              startTs + tickMills,
              graph,
              workers.values().stream().min(Integer::compareTo).get(),
              workers,
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

  }

  public Consumer<Object> roundRobinFronts(int n) {
    final Map< = createFronts(n);
    return new Co
  }

  public Consumer<Object> randomFrontConsumer(int maxFrontsCount) {
    final Set<Integer> fronts = innerEnvironment.availableFronts();
    final List<Consumer<Object>> collectors = fronts.stream()
            .map(innerEnvironment::frontConsumer)
            .limit(maxFrontsCount)
            .collect(Collectors.toList());

    final Random rd = new Random();
    return obj -> collectors.get(rd.nextInt(collectors.size())).accept(obj);
  }

  private Map<HashRange, Integer> rangeMappingForTick() {
    final Map<HashRange, Integer> result = new HashMap<>();
    final Set<Integer> workerIds = innerEnvironment.availableWorkers();

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

  public void awaitTick(int seconds) {
    try {
      SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      innerEnvironment.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deploy(TickInfo tickInfo) {
    innerEnvironment.deploy(tickInfo);
  }

  @Override
  public void deployFront(int nodeId, int frontId, Props frontProps) {
    innerEnvironment.deployFront(nodeId, frontId, frontProps);
  }

  @Override
  public Set<Integer> availableWorkers() {
    return innerEnvironment.availableWorkers();
  }

  @Override
  public <T> AtomicGraph wrapInSink(ToIntFunction<? super T> hash, Consumer<? super T> mySuperConsumer) {
    return innerEnvironment.wrapInSink(hash, mySuperConsumer);
  }

  public Consumer<Object> frontConsumer(int frontId) {

  }
}
