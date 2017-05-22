package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.FakeAtomicHandle;
import com.spbsu.datastream.core.GlobalTime;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.Meta;
import com.spbsu.datastream.core.PayloadDataItem;
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
    final DataItem<String> x1 = new PayloadDataItem<>(new Meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x3 = new PayloadDataItem<>(new Meta(new GlobalTime(3, 1)), "v3");

    final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(x1, x2, x3), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x1.payload());
    final List<String> y2 = Arrays.asList(x1.payload(), x2.payload());
    final List<String> y3 = Arrays.asList(x2.payload(), x3.payload());

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    System.out.println(actualResult);
    System.out.println(expectedResult);
    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void cycleSimulation() {
    final Meta x1Meta = new Meta(new GlobalTime(1, 1));

    final DataItem<String> x1 = new PayloadDataItem<>(x1Meta, "v1");
    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x1Prime = new PayloadDataItem<>(new Meta(x1Meta, 2), "state");

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
    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x1 = new PayloadDataItem<>(new Meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x3 = new PayloadDataItem<>(new Meta(new GlobalTime(3, 1)), "v3");

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
    final DataItem<String> x1 = new PayloadDataItem<>(new Meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x3 = new PayloadDataItem<>(new Meta(new GlobalTime(3, 1)), "v3");
    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");

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
    final DataItem<String> x3 = new PayloadDataItem<>(new Meta(new GlobalTime(3, 1)), "v3");
    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x1 = new PayloadDataItem<>(new Meta(new GlobalTime(1, 1)), "v1");

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
    final DataItem<String> x1 = new PayloadDataItem<>(new Meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");

    final Meta x3Meta = new Meta(new GlobalTime(3, 1));
    final DataItem<String> x3 = new PayloadDataItem<>(new Meta(x3Meta, 1), "v3");
    final DataItem<String> x3Prime = new PayloadDataItem<>(new Meta(x3Meta, 2), "v3Prime");

    final List<List<String>> actualResult = GroupingTest.groupMe(Arrays.asList(x1, x2, x3, x3Prime), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x1.payload());
    final List<String> y2 = Arrays.asList(x1.payload(), x2.payload());
    final List<String> y3 = Arrays.asList(x2.payload(), x3.payload());
    final List<String> y4 = Arrays.asList(x2.payload(), x3Prime.payload());
    //Invalidation element should replace invalid
    // {@link com.spbsu.datastreams.core.Trace#isInvalidatedBy method}
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
            .mapToObj(i -> new PayloadDataItem<>(new Meta(new GlobalTime(i, 1)), "v" + i))
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

  private static <T> List<List<T>> groupMe(Iterable<DataItem<T>> input, int window) {
    final Grouping<T> grouping = new Grouping<>(HashFunction.constantHash(1), window);

    final List<List<T>> out = new ArrayList<>();

    final AtomicHandle handle = new FakeAtomicHandle((port, di) -> out.add((List<T>) di.payload()));

    grouping.onStart(handle);
    input.forEach(in -> grouping.onPush(grouping.inPort(), in, handle));
    return out;
  }
}
