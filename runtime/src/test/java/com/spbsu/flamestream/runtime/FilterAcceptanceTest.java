package com.spbsu.flamestream.runtime;

import com.spbsu.flamestream.common.FlameStreamSuite;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.Graph;
import com.spbsu.flamestream.core.graph.HashFunction;
import com.spbsu.flamestream.core.graph.barrier.BarrierSuite;
import com.spbsu.flamestream.core.graph.ops.StatelessMap;
import com.spbsu.flamestream.core.graph.source.AbstractSource;
import com.spbsu.flamestream.core.graph.source.SimpleSource;
import com.spbsu.flamestream.runtime.environment.local.LocalClusterEnvironment;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class FilterAcceptanceTest extends FlameStreamSuite {

  private static Graph multiGraph(AtomicGraph sink) {
    final AbstractSource source = new SimpleSource();
    final StatelessMap<Integer, Integer> filter1 = new StatelessMap<>(new HumbleFiler(-1), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter2 = new StatelessMap<>(new HumbleFiler(-2), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter3 = new StatelessMap<>(new HumbleFiler(-3), HashFunction.OBJECT_HASH);
    final StatelessMap<Integer, Integer> filter4 = new StatelessMap<>(new HumbleFiler(-4), HashFunction.OBJECT_HASH);

    final BarrierSuite<Integer> barrier = new BarrierSuite<>(sink);

    return source.fuse(filter1, source.outPort(), filter1.inPort())
            .fuse(filter2, filter1.outPort(), filter2.inPort())
            .fuse(filter3, filter2.outPort(), filter3.inPort())
            .fuse(filter4, filter3.outPort(), filter4.inPort())
            .fuse(barrier, filter4.outPort(), barrier.inPort());
  }

  @Test
  public void linearFilter() {
    try (LocalClusterEnvironment lce = new LocalClusterEnvironment(4);
            TestEnvironment environment = new TestEnvironment(lce)) {
      final Queue<Integer> result = new ArrayDeque<>();
      final Consumer<Object> sink = environment.deploy(FilterAcceptanceTest.multiGraph(
              environment.wrapInSink(HashFunction.OBJECT_HASH, result::add)
      ).flattened(), 15, 1, 4);

      final List<Integer> source = new Random().ints(1000).boxed().collect(Collectors.toList());
      source.forEach(sink);

      environment.awaitTicks();

      Assert.assertEquals(
              new HashSet<>(result),
              source.stream().map(str -> str * -1 * -2 * -3 * -4).collect(Collectors.toSet())
      );
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test(enabled = false)
  public void multipleTicksLinearFilter() {
    try (LocalClusterEnvironment lce = new LocalClusterEnvironment(4);
            TestEnvironment environment = new TestEnvironment(lce)) {
      final Queue<Integer> result = new ArrayDeque<>();
      final Consumer<Object> sink = environment.deploy(FilterAcceptanceTest.multiGraph(
              environment.wrapInSink(HashFunction.OBJECT_HASH, result::add)
      ).flattened(), 2, 10, 4);

      final List<Integer> source = new Random().ints(20000).boxed().collect(Collectors.toList());
      source.forEach(sink);

      environment.awaitTicks();

      Assert.assertEquals(
              new HashSet<>(result),
              source.stream().map(str -> str * -1 * -2 * -3 * -4).collect(Collectors.toSet())
      );
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  static final class HumbleFiler implements Function<Integer, Integer> {
    private final int factor;

    HumbleFiler(int factor) {
      this.factor = factor;
    }

    @Override
    public Integer apply(Integer s) {
      return s * factor;
    }
  }
}
