package com.spbsu.flamestream.core.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.invalidation.ArrayInvalidatingBucket;
import com.spbsu.flamestream.core.data.invalidation.InvalidatingBucket;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import org.jooq.lambda.Collectable;
import org.jooq.lambda.Seq;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class GroupingTest extends FlameStreamSuite {

  private static List<List<String>> groupMe(Stream<DataItem> input, int window) {
    final Map<DataItem, InvalidatingBucket> state = new HashMap<>();
    final Grouping<String> grouping = new Grouping<>(
            HashFunction.constantHash(1),
            (t, t2) -> true,
            window,
            String.class
    );
    final BiFunction<DataItem, InvalidatingBucket, Stream<DataItem>> operation = grouping.operation();

    //noinspection unchecked
    return input
            .map(di -> new DataItemForTest(di, grouping.hash(), grouping.equalz()))
            .flatMap(di -> {
              state.putIfAbsent(di, new ArrayInvalidatingBucket());
              return operation.apply(di, state.get(di));
            })
            .map(dataItem -> dataItem.payload(List.class))
            .map(element -> (List<String>) element)
            .collect(Collectors.toList());
  }

  @Test
  public void withoutReordering() {
    final DataItem x1 = new PayloadDataItem(Meta.meta(new GlobalTime(1, EdgeId.MIN)), "v1");
    final DataItem x2 = new PayloadDataItem(Meta.meta(new GlobalTime(2, EdgeId.MIN)), "v2");
    final DataItem x3 = new PayloadDataItem(Meta.meta(new GlobalTime(3, EdgeId.MIN)), "v3");

    final List<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x1, x2, x3), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x1.payload(String.class));
    final List<String> y2 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));
    final List<String> y3 = Arrays.asList(x2.payload(String.class), x3.payload(String.class));

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void groupWithSuccessor() {
    final Meta x5Global = Meta.meta(new GlobalTime(2, EdgeId.MIN));
    final Meta x0Global = Meta.meta(new GlobalTime(1, EdgeId.MIN));

    final DataItem x5 = new PayloadDataItem(x5Global, "v5");
    final DataItem x5State = new PayloadDataItem(x5Global.advanced(1), "v5State");
    final DataItem x0 = new PayloadDataItem(x0Global, "x0");
    final DataItem x0State = new PayloadDataItem(x0Global.advanced(2), "x0State");
    final DataItem theState = new PayloadDataItem(x5Global.advanced(3), "theState");

    final List<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x5, x5State, x0, x0State, theState), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x5.payload(String.class));
    final List<String> y2 = Arrays.asList(x5.payload(String.class), x5State.payload(String.class));
    final List<String> y3 = Collections.singletonList(x0.payload(String.class));
    final List<String> y4 = Arrays.asList(x0.payload(String.class), x5.payload(String.class));
    final List<String> y5 = Arrays.asList(x0.payload(String.class), x0State.payload(String.class));
    final List<String> y6 = Arrays.asList(x0State.payload(String.class), x5.payload(String.class));
    final List<String> y7 = Arrays.asList(x5.payload(String.class), theState.payload(String.class));

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
    final Meta x1Meta = Meta.meta(new GlobalTime(1, EdgeId.MIN));

    final DataItem x1 = new PayloadDataItem(x1Meta, "v1");
    final DataItem x2 = new PayloadDataItem(Meta.meta(new GlobalTime(2, EdgeId.MIN)), "v2");
    final DataItem x1Prime = new PayloadDataItem(x1Meta.advanced(2), "state");

    final List<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x1, x2, x1Prime), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x1.payload(String.class));
    final List<String> y2 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));
    final List<String> y3 = Arrays.asList(x1.payload(String.class), x1Prime.payload(String.class));
    final List<String> y4 = Arrays.asList(x1Prime.payload(String.class), x2.payload(String.class));

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void headReordering() {
    final DataItem x2 = new PayloadDataItem(Meta.meta(new GlobalTime(2, EdgeId.MIN)), "v2");
    final DataItem x1 = new PayloadDataItem(Meta.meta(new GlobalTime(1, EdgeId.MIN)), "v1");
    final DataItem x3 = new PayloadDataItem(Meta.meta(new GlobalTime(3, EdgeId.MIN)), "v3");

    final List<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x2, x1, x3), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x2.payload(String.class));
    final List<String> y2 = Collections.singletonList(x1.payload(String.class));
    final List<String> y3 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));
    final List<String> y4 = Arrays.asList(x2.payload(String.class), x3.payload(String.class));

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void tailReordering() {
    final DataItem x1 = new PayloadDataItem(Meta.meta(new GlobalTime(1, EdgeId.MIN)), "v1");
    final DataItem x3 = new PayloadDataItem(Meta.meta(new GlobalTime(3, EdgeId.MIN)), "v3");
    final DataItem x2 = new PayloadDataItem(Meta.meta(new GlobalTime(2, EdgeId.MIN)), "v2");

    final List<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x1, x3, x2), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x1.payload(String.class));
    final List<String> y2 = Arrays.asList(x1.payload(String.class), x3.payload(String.class));
    final List<String> y3 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));
    final List<String> y4 = Arrays.asList(x2.payload(String.class), x3.payload(String.class));

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void reverseReordering() {
    final DataItem x3 = new PayloadDataItem(Meta.meta(new GlobalTime(3, EdgeId.MIN)), "v3");
    final DataItem x2 = new PayloadDataItem(Meta.meta(new GlobalTime(2, EdgeId.MIN)), "v2");
    final DataItem x1 = new PayloadDataItem(Meta.meta(new GlobalTime(1, EdgeId.MIN)), "v1");

    final List<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x3, x2, x1), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x3.payload(String.class));
    final List<String> y2 = Collections.singletonList(x2.payload(String.class));
    final List<String> y3 = Arrays.asList(x2.payload(String.class), x3.payload(String.class));
    final List<String> y4 = Collections.singletonList(x1.payload(String.class));
    final List<String> y5 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));
    //This pair shouldn't be replayed. Crucial optimization
    //final List<String> y6 = new List<>(Arrays.asList(x2.payload(String.class), x3.payload(String.class)), 1);

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
    final DataItem x1 = new PayloadDataItem(Meta.meta(new GlobalTime(1, EdgeId.MIN)), "v1");
    final DataItem x2 = new PayloadDataItem(Meta.meta(new GlobalTime(2, EdgeId.MIN)), "v2");

    final Meta x3Meta = Meta.meta(new GlobalTime(3, EdgeId.MIN));
    final DataItem x3 = new PayloadDataItem(x3Meta.advanced(1), "v3");
    final DataItem x3Prime = new PayloadDataItem(x3Meta.advanced(2), "v3Prime");

    final List<List<String>> actualResult = GroupingTest.groupMe(Stream.of(x1, x2, x3, x3Prime), 2);
    final List<List<String>> expectedResult = new ArrayList<>();

    final List<String> y1 = Collections.singletonList(x1.payload(String.class));
    final List<String> y2 = Arrays.asList(x1.payload(String.class), x2.payload(String.class));
    final List<String> y3 = Arrays.asList(x2.payload(String.class), x3.payload(String.class));
    final List<String> y4 = Arrays.asList(x2.payload(String.class), x3Prime.payload(String.class));
    //Invalidation element should replace invalid
    // {@link com.spbsu.flamestream.core.TraceImpl#isInvalidatedBy method}
    //final List<String> y5 = new List<>(Arrays.asList(x3Prime.payload(String.class), x3.payload(String.class)), 1);

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);
    //expectedResult.add(y5);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void shuffleReordering() {

    final List<DataItem> input = IntStream.range(0, 1000)
            .mapToObj(i -> new PayloadDataItem(Meta.meta(new GlobalTime(i, EdgeId.MIN)), "v" + i))
            .collect(Collectors.toList());

    final List<DataItem> shuffledInput = new ArrayList<>(input);
    Collections.shuffle(shuffledInput, new Random(2));

    final int window = 6;
    final Set<List<String>> mustHave = Seq.seq(input)
            .map(dataItem -> dataItem.payload(String.class))
            .sliding(window)
            .map(Collectable::toList)
            .toSet();

    //this.LOG.debug("Got: {}", out.stream().map(DataItem::payload).collect(Collectors.toList()));
    //this.LOG.debug("Must have: {}", mustHave);

    Assert.assertTrue(
            new HashSet<>(GroupingTest.groupMe(shuffledInput.stream(), window)).containsAll(mustHave),
            "Result must contain expected elements"
    );
  }

  @Test(enabled = false)
  public void brothersInvalidation() {
    final DataItem father = new PayloadDataItem(Meta.meta(new GlobalTime(1, EdgeId.MIN)), "father");

    final DataItem son1 = new PayloadDataItem(father.meta().advanced(1, 0), "son1");
    final DataItem son2 = new PayloadDataItem(father.meta().advanced(1, 1), "son2");
    final DataItem son3 = new PayloadDataItem(father.meta().advanced(1, 2), "son3");

    final DataItem son1Prime = new PayloadDataItem(father.meta().advanced(2, 0), "son1Prime");
    final DataItem son2Prime = new PayloadDataItem(father.meta().advanced(2, 1), "son2Prime");
    final DataItem son3Prime = new PayloadDataItem(father.meta().advanced(2, 2), "son3Prime");

    { //without reordering
      final List<List<String>> actualResult = GroupingTest.groupMe(Stream.of(
              son1,
              son2,
              son3,
              son1Prime,
              son2Prime,
              son3Prime
      ), 2);
      final List<List<String>> expectedResult = Arrays.asList(
              Collections.singletonList(son1.payload(String.class)),
              Arrays.asList(son1.payload(String.class), son2.payload(String.class)),
              Arrays.asList(son2.payload(String.class), son3.payload(String.class)),
              Collections.singletonList(son1Prime.payload(String.class)),
              Arrays.asList(son1Prime.payload(String.class), son2Prime.payload(String.class)),
              Arrays.asList(son2Prime.payload(String.class), son3Prime.payload(String.class))
      );
      Assert.assertEquals(actualResult, expectedResult, "Brothers invalidation without reordering");
    }
    { //with reordering #1
      final List<List<String>> actualResult = GroupingTest.groupMe(Stream.of(
              son1,
              son2,
              son1Prime,
              son3,
              son2Prime,
              son3Prime
      ), 2);
      final List<List<String>> expectedResult = Arrays.asList(
              Collections.singletonList(son1.payload(String.class)),
              Arrays.asList(son1.payload(String.class), son2.payload(String.class)),
              Collections.singletonList(son1Prime.payload(String.class)),
              Arrays.asList(son1Prime.payload(String.class), son2Prime.payload(String.class)),
              Arrays.asList(son2Prime.payload(String.class), son3Prime.payload(String.class))
      );
      Assert.assertEquals(actualResult, expectedResult, "Brothers invalidation with reordering #1");
    }
    { //with reordering #2
      final List<List<String>> actualResult = GroupingTest.groupMe(Stream.of(
              son1Prime,
              son1,
              son2,
              son2Prime,
              son3,
              son3Prime
      ), 2);
      final List<List<String>> expectedResult = Arrays.asList(
              Collections.singletonList(son1Prime.payload(String.class)),
              Arrays.asList(son1Prime.payload(String.class), son2Prime.payload(String.class)),
              Arrays.asList(son2Prime.payload(String.class), son3Prime.payload(String.class))
      );
      Assert.assertEquals(actualResult, expectedResult, "Brothers invalidation with reordering #2");
    }
  }
}
