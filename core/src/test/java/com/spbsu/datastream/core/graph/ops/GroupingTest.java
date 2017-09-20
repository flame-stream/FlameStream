package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.FakeAtomicHandle;
import com.spbsu.datastream.core.meta.GlobalTime;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.meta.Meta;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;
import org.jooq.lambda.Collectable;
import org.jooq.lambda.Seq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("unchecked")
public final class GroupingTest {

  private final Logger LOG = LoggerFactory.getLogger(GroupingTest.class);

  @Test
  public void withoutReordering() {
    final DataItem<String> x1 = new PayloadDataItem<>(Meta.meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x2 = new PayloadDataItem<>(Meta.meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x3 = new PayloadDataItem<>(Meta.meta(new GlobalTime(3, 1)), "v3");

    final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(x1, x2, x3), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x1.payload());
    final List<String> y2 = Arrays.asList(x1.payload(), x2.payload());
    final List<String> y3 = Arrays.asList(x2.payload(), x3.payload());

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void groupWithSuccessor() {
    final Meta x5Global = Meta.meta(new GlobalTime(2, 1));
    final Meta x0Global = Meta.meta(new GlobalTime(1, 1));

    final DataItem<String> x5 = new PayloadDataItem<>(x5Global, "v5");
    final DataItem<String> x5State = new PayloadDataItem<>(x5Global.advanced(1), "v5State");
    final DataItem<String> x0 = new PayloadDataItem<>(x0Global, "x0");
    final DataItem<String> x0State = new PayloadDataItem<>(x0Global.advanced(2), "x0State");
    final DataItem<String> theState = new PayloadDataItem<>(x5Global.advanced(3), "theState");

    final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(x5, x5State, x0, x0State, theState), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x5.payload());
    final List<String> y2 = Arrays.asList(x5.payload(), x5State.payload());
    final List<String> y3 = Collections.singletonList(x0.payload());
    final List<String> y4 = Arrays.asList(x0.payload(), x5.payload());
    final List<String> y5 = Arrays.asList(x0.payload(), x0State.payload());
    final List<String> y6 = Arrays.asList(x0State.payload(), x5.payload());
    final List<String> y7 = Arrays.asList(x5.payload(), theState.payload());

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);
    expectedResult.add(y5);
    expectedResult.add(y6);
    expectedResult.add(y7);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void cycleSimulation() {
    final Meta x1Meta = Meta.meta(new GlobalTime(1, 1));

    final DataItem<String> x1 = new PayloadDataItem<>(x1Meta, "v1");
    final DataItem<String> x2 = new PayloadDataItem<>(Meta.meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x1Prime = new PayloadDataItem<>(x1Meta.advanced(2), "state");

    final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(x1, x2, x1Prime), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x1.payload());
    final List<String> y2 = Arrays.asList(x1.payload(), x2.payload());
    final List<String> y3 = Arrays.asList(x1.payload(), x1Prime.payload());
    final List<String> y4 = Arrays.asList(x1Prime.payload(), x2.payload());

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void headReordering() {
    final DataItem<String> x2 = new PayloadDataItem<>(Meta.meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x1 = new PayloadDataItem<>(Meta.meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x3 = new PayloadDataItem<>(Meta.meta(new GlobalTime(3, 1)), "v3");

    final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(x2, x1, x3), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x2.payload());
    final List<String> y2 = Collections.singletonList(x1.payload());
    final List<String> y3 = Arrays.asList(x1.payload(), x2.payload());
    final List<String> y4 = Arrays.asList(x2.payload(), x3.payload());

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void tailReordering() {
    final DataItem<String> x1 = new PayloadDataItem<>(Meta.meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x3 = new PayloadDataItem<>(Meta.meta(new GlobalTime(3, 1)), "v3");
    final DataItem<String> x2 = new PayloadDataItem<>(Meta.meta(new GlobalTime(2, 1)), "v2");

    final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(x1, x3, x2), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x1.payload());
    final List<String> y2 = Arrays.asList(x1.payload(), x3.payload());
    final List<String> y3 = Arrays.asList(x1.payload(), x2.payload());
    final List<String> y4 = Arrays.asList(x2.payload(), x3.payload());

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void reverseReordering() {
    final DataItem<String> x3 = new PayloadDataItem<>(Meta.meta(new GlobalTime(3, 1)), "v3");
    final DataItem<String> x2 = new PayloadDataItem<>(Meta.meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x1 = new PayloadDataItem<>(Meta.meta(new GlobalTime(1, 1)), "v1");

    final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(x3, x2, x1), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x3.payload());
    final List<String> y2 = Collections.singletonList(x2.payload());
    final List<String> y3 = Arrays.asList(x2.payload(), x3.payload());
    final List<String> y4 = Collections.singletonList(x1.payload());
    final List<String> y5 = Arrays.asList(x1.payload(), x2.payload());
    //This pair shouldn't be replayed. Crucial optimization
    //final List<String> y6 = new List<>(Arrays.asList(x2.payload(), x3.payload()), 1);

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);
    expectedResult.add(y5);
    //expectedResult.add(y6);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void reorderingWithInvalidating() {
    final DataItem<String> x1 = new PayloadDataItem<>(Meta.meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x2 = new PayloadDataItem<>(Meta.meta(new GlobalTime(2, 1)), "v2");

    final Meta x3Meta = Meta.meta(new GlobalTime(3, 1));
    final DataItem<String> x3 = new PayloadDataItem<>(x3Meta.advanced(1), "v3");
    final DataItem<String> x3Prime = new PayloadDataItem<>(x3Meta.advanced(2), "v3Prime");

    final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(x1, x2, x3, x3Prime), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x1.payload());
    final List<String> y2 = Arrays.asList(x1.payload(), x2.payload());
    final List<String> y3 = Arrays.asList(x2.payload(), x3.payload());
    final List<String> y4 = Arrays.asList(x2.payload(), x3Prime.payload());
    //Invalidation element should replace invalid
    // {@link com.spbsu.datastreams.core.TraceImpl#isInvalidatedBy method}
    //final List<String> y5 = new List<>(Arrays.asList(x3Prime.payload(), x3.payload()), 1);

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);
    //expectedResult.add(y5);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void shuffleReordering() {

    final List<DataItem<String>> input = IntStream.range(0, 1000)
            .mapToObj(i -> new PayloadDataItem<>(Meta.meta(new GlobalTime(i, 1)), "v" + i))
            .collect(Collectors.toList());

    final List<DataItem<String>> shuffledInput = new ArrayList<>(input);
    Collections.shuffle(shuffledInput, new Random(2));

    final int window = 6;
    final Set<List<String>> mustHave = Seq.seq(input)
            .map(DataItem::payload)
            .sliding(window)
            .map(Collectable::toList)
            .toSet();

    //this.LOG.debug("Got: {}", out.stream().map(DataItem::payload).collect(Collectors.toList()));
    //this.LOG.debug("Must have: {}", mustHave);

    Assert.assertTrue(new HashSet<>(GroupingTest.groupMe(shuffledInput, window)).containsAll(mustHave), "Result must contain expected elements");
  }

  @Test(enabled = false)
  public void brothersInvalidation() {
    final DataItem<String> father = new PayloadDataItem<>(Meta.meta(new GlobalTime(1, 1)), "father");

    final DataItem<String> son1 = new PayloadDataItem<>(father.meta().advanced(1, 0), "son1");
    final DataItem<String> son2 = new PayloadDataItem<>(father.meta().advanced(1, 1), "son2");
    final DataItem<String> son3 = new PayloadDataItem<>(father.meta().advanced(1, 2), "son3");

    final DataItem<String> son1Prime = new PayloadDataItem<>(father.meta().advanced(2, 0), "son1Prime");
    final DataItem<String> son2Prime = new PayloadDataItem<>(father.meta().advanced(2, 1), "son2Prime");
    final DataItem<String> son3Prime = new PayloadDataItem<>(father.meta().advanced(2, 2), "son3Prime");

    { //without reordering
      final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(son1, son2, son3, son1Prime, son2Prime, son3Prime), 2);
      final List<List<String>> expectedResult = Arrays.asList(
              Collections.singletonList(son1.payload()),
              Arrays.asList(son1.payload(), son2.payload()),
              Arrays.asList(son2.payload(), son3.payload()),
              Collections.singletonList(son1Prime.payload()),
              Arrays.asList(son1Prime.payload(), son2Prime.payload()),
              Arrays.asList(son2Prime.payload(), son3Prime.payload())
      );
      Assert.assertEquals(actualResult, expectedResult, "Brothers invalidation without reordering");
    }
    { //with reordering #1
      final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(son1, son2, son1Prime, son3, son2Prime, son3Prime), 2);
      final List<List<String>> expectedResult = Arrays.asList(
              Collections.singletonList(son1.payload()),
              Arrays.asList(son1.payload(), son2.payload()),
              Collections.singletonList(son1Prime.payload()),
              Arrays.asList(son1Prime.payload(), son2Prime.payload()),
              Arrays.asList(son2Prime.payload(), son3Prime.payload())
      );
      Assert.assertEquals(actualResult, expectedResult, "Brothers invalidation with reordering #1");
    }
    { //with reordering #2
      final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(son1Prime, son1, son2, son2Prime, son3, son3Prime), 2);
      final List<List<String>> expectedResult = Arrays.asList(
              Collections.singletonList(son1Prime.payload()),
              Arrays.asList(son1Prime.payload(), son2Prime.payload()),
              Arrays.asList(son2Prime.payload(), son3Prime.payload())
      );
      Assert.assertEquals(actualResult, expectedResult, "Brothers invalidation with reordering #2");
    }
  }

  private static <T> List<List<T>> groupMe(Iterable<DataItem<T>> input, int window) {
    final Grouping<T> grouping = new Grouping<>(HashFunction.constantHash(1), (t, t2) -> true, window);

    final List<List<T>> out = new ArrayList<>();

    final AtomicHandle handle = new FakeAtomicHandle((port, di) -> out.add((List<T>) di.payload()));

    grouping.onStart(handle);
    input.forEach(in -> grouping.onPush(grouping.inPort(), in, handle));
    return out;
  }
}
