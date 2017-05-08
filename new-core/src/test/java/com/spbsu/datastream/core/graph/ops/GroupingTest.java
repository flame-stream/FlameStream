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
    final Grouping<String> grouping = new Grouping<>(HashFunction.constantHash(1), 2);

    final List<DataItem<GroupingResult<String>>> out = new ArrayList<>();

    final AtomicHandle handle = new FakeAtomicHandle((port, di) -> out.add((DataItem<GroupingResult<String>>) di));

    final DataItem<String> x1 = new PayloadDataItem<>(new Meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x3 = new PayloadDataItem<>(new Meta(new GlobalTime(3, 1)), "v3");

    grouping.onStart(handle);
    grouping.onPush(grouping.inPort(), x1, handle);
    grouping.onPush(grouping.inPort(), x2, handle);
    grouping.onPush(grouping.inPort(), x3, handle);

    final List<GroupingResult<String>> actualResult = out.stream().map(DataItem::payload).collect(Collectors.toList());
    final List<GroupingResult<String>> expectedResult = new ArrayList<>();

    final GroupingResult<String> y1 = new GroupingResult<>(Collections.singletonList(x1.payload()), 1);
    final GroupingResult<String> y2 = new GroupingResult<>(Arrays.asList(x1.payload(), x2.payload()), 1);
    final GroupingResult<String> y3 = new GroupingResult<>(Arrays.asList(x2.payload(), x3.payload()), 1);

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void headReordering() {
    final Grouping<String> grouping = new Grouping<>(HashFunction.constantHash(1), 2);

    final List<DataItem<GroupingResult<String>>> out = new ArrayList<>();

    final AtomicHandle handle = new FakeAtomicHandle((port, di) -> out.add((DataItem<GroupingResult<String>>) di));

    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x1 = new PayloadDataItem<>(new Meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x3 = new PayloadDataItem<>(new Meta(new GlobalTime(3, 1)), "v3");

    grouping.onStart(handle);
    grouping.onPush(grouping.inPort(), x2, handle);
    grouping.onPush(grouping.inPort(), x1, handle);
    grouping.onPush(grouping.inPort(), x3, handle);

    final List<GroupingResult<String>> actualResult = out.stream().map(DataItem::payload).collect(Collectors.toList());
    final List<GroupingResult<String>> expectedResult = new ArrayList<>();

    final GroupingResult<String> y1 = new GroupingResult<>(Collections.singletonList(x2.payload()), 1);
    final GroupingResult<String> y2 = new GroupingResult<>(Collections.singletonList(x1.payload()), 1);
    final GroupingResult<String> y3 = new GroupingResult<>(Arrays.asList(x1.payload(), x2.payload()), 1);
    final GroupingResult<String> y4 = new GroupingResult<>(Arrays.asList(x2.payload(), x3.payload()), 1);

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void tailReordering() {
    final Grouping<String> grouping = new Grouping<>(HashFunction.constantHash(1), 2);

    final List<DataItem<GroupingResult<String>>> out = new ArrayList<>();

    final AtomicHandle handle = new FakeAtomicHandle((port, di) -> out.add((DataItem<GroupingResult<String>>) di));

    final DataItem<String> x1 = new PayloadDataItem<>(new Meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x3 = new PayloadDataItem<>(new Meta(new GlobalTime(3, 1)), "v3");
    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");

    grouping.onStart(handle);
    grouping.onPush(grouping.inPort(), x1, handle);
    grouping.onPush(grouping.inPort(), x3, handle);
    grouping.onPush(grouping.inPort(), x2, handle);

    final List<GroupingResult<String>> actualResult = out.stream().map(DataItem::payload).collect(Collectors.toList());
    final List<GroupingResult<String>> expectedResult = new ArrayList<>();

    final GroupingResult<String> y1 = new GroupingResult<>(Collections.singletonList(x1.payload()), 1);
    final GroupingResult<String> y2 = new GroupingResult<>(Arrays.asList(x1.payload(), x3.payload()), 1);
    final GroupingResult<String> y3 = new GroupingResult<>(Arrays.asList(x1.payload(), x2.payload()), 1);
    final GroupingResult<String> y4 = new GroupingResult<>(Arrays.asList(x2.payload(), x3.payload()), 1);

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void reverseReordering() {
    final Grouping<String> grouping = new Grouping<>(HashFunction.constantHash(1), 2);

    final List<DataItem<GroupingResult<String>>> out = new ArrayList<>();

    final AtomicHandle handle = new FakeAtomicHandle((port, di) -> out.add((DataItem<GroupingResult<String>>) di));

    final DataItem<String> x1 = new PayloadDataItem<>(new Meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");
    final DataItem<String> x3 = new PayloadDataItem<>(new Meta(new GlobalTime(3, 1)), "v3");

    grouping.onStart(handle);
    grouping.onPush(grouping.inPort(), x3, handle);
    grouping.onPush(grouping.inPort(), x2, handle);
    grouping.onPush(grouping.inPort(), x1, handle);

    final List<GroupingResult<String>> actualResult = out.stream().map(DataItem::payload).collect(Collectors.toList());
    final List<GroupingResult<String>> expectedResult = new ArrayList<>();

    final GroupingResult<String> y1 = new GroupingResult<>(Collections.singletonList(x3.payload()), 1);
    final GroupingResult<String> y2 = new GroupingResult<>(Collections.singletonList(x2.payload()), 1);
    final GroupingResult<String> y3 = new GroupingResult<>(Arrays.asList(x2.payload(), x3.payload()), 1);
    final GroupingResult<String> y4 = new GroupingResult<>(Collections.singletonList(x1.payload()), 1);
    final GroupingResult<String> y5 = new GroupingResult<>(Arrays.asList(x1.payload(), x2.payload()), 1);
    final GroupingResult<String> y6 = new GroupingResult<>(Arrays.asList(x2.payload(), x3.payload()), 1);

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);
    expectedResult.add(y5);
    expectedResult.add(y6);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void reorderingWithInvalidating() {
    final Grouping<String> grouping = new Grouping<>(HashFunction.constantHash(1), 2);

    final List<DataItem<GroupingResult<String>>> out = new ArrayList<>();

    final AtomicHandle handle = new FakeAtomicHandle((port, di) -> out.add((DataItem<GroupingResult<String>>) di));

    final DataItem<String> x1 = new PayloadDataItem<>(new Meta(new GlobalTime(1, 1)), "v1");
    final DataItem<String> x2 = new PayloadDataItem<>(new Meta(new GlobalTime(2, 1)), "v2");

    final Meta x3Meta = new Meta(new GlobalTime(3, 1));
    final DataItem<String> x3 = new PayloadDataItem<>(new Meta(x3Meta, 1), "v3");
    final DataItem<String> x3Prime = new PayloadDataItem<>(new Meta(x3Meta, 2), "v3Prime");

    grouping.onStart(handle);
    grouping.onPush(grouping.inPort(), x1, handle);
    grouping.onPush(grouping.inPort(), x2, handle);
    grouping.onPush(grouping.inPort(), x3, handle);
    grouping.onPush(grouping.inPort(), x3Prime, handle);

    final List<GroupingResult<String>> actualResult = out.stream().map(DataItem::payload).collect(Collectors.toList());
    final List<GroupingResult<String>> expectedResult = new ArrayList<>();

    final GroupingResult<String> y1 = new GroupingResult<>(Collections.singletonList(x1.payload()), 1);
    final GroupingResult<String> y2 = new GroupingResult<>(Arrays.asList(x1.payload(), x2.payload()), 1);
    final GroupingResult<String> y3 = new GroupingResult<>(Arrays.asList(x2.payload(), x3.payload()), 1);
    final GroupingResult<String> y4 = new GroupingResult<>(Arrays.asList(x2.payload(), x3Prime.payload()), 1);
    final GroupingResult<String> y5 = new GroupingResult<>(Arrays.asList(x3Prime.payload(), x3.payload()), 1);

    expectedResult.add(y1);
    expectedResult.add(y2);
    expectedResult.add(y3);
    expectedResult.add(y4);
    expectedResult.add(y5);

    Assert.assertEquals(actualResult, expectedResult);
  }

  @Test
  public void shuffleReordering() {
    final int window = 6;

    final Grouping<String> grouping = new Grouping<>(HashFunction.constantHash(1), window);

    final List<DataItem<GroupingResult<String>>> out = new ArrayList<>();

    final AtomicHandle handle = new FakeAtomicHandle((port, di) -> out.add((DataItem<GroupingResult<String>>) di));

    final List<DataItem<String>> input = IntStream.range(0, 1000)
            .mapToObj(i -> new PayloadDataItem<>(new Meta(new GlobalTime(i, 1)), "v" + i))
            .collect(Collectors.toList());

    final List<DataItem<String>> shuffledInput = new ArrayList<>(input);

    Collections.shuffle(shuffledInput, new Random(2));

    grouping.onStart(handle);
    shuffledInput.forEach(di -> grouping.onPush(grouping.inPort(), di, handle));

    final Set<GroupingResult<String>> mustHave = Seq.seq(input)
            .map(DataItem::payload)
            .sliding(window)
            .map(Collectable::toList)
            .map(li -> new GroupingResult<>(li, 1))
            .toSet();

    //this.LOG.debug("Got: {}", out.stream().map(DataItem::payload).collect(Collectors.toList()));
    //this.LOG.debug("Must have: {}", mustHave);

    Assert.assertTrue(out.stream().map(DataItem::payload).collect(Collectors.toSet()).containsAll(mustHave), "Result must contain expected elements");
  }
}