package com.spbsu.datastream.core.ack.impl;

import com.spbsu.commons.util.Pair;
import com.spbsu.datastream.core.ack.AckTable;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 30.08.2017
 */
public class AckTableTest {

  @DataProvider
  public Object[][] logicTestProvider() {
    final long startTs = 0;
    final long window = 10;
    final long windowsCount = 40 * 1000;
    final long items = 100 * 1000;
    return new Object[][]{
            {new ArrayAckTable(startTs, window * windowsCount, window), window, windowsCount, items},
            {new TreeAckTable(startTs, window), window, windowsCount, items}
    };
  }

  @Test(dataProvider = "logicTestProvider")
  public void logicTest(AckTable ackTable, long window, long windowsCount, long items) {
    final List<Pair<Long, Long>> xors = Stream.generate(() -> ThreadLocalRandom.current().nextLong(1, window * windowsCount))
            .distinct()
            .limit(items)
            .map(ts -> new Pair<>(ts, ThreadLocalRandom.current().nextLong()))
            .collect(Collectors.toList());

    LongStream.range(0, windowsCount).forEach(value -> ackTable.report(window * value, 0));
    final SortedSet<Long> sortedSet = new TreeSet<>();
    xors.forEach(pair -> {
      ackTable.ack(pair.first, pair.second);
      sortedSet.add(pair.first);
      Assert.assertEquals(ackTable.min(), window * (sortedSet.first() / window));
    });
    Collections.shuffle(xors);
    xors.forEach(pair -> {
      Assert.assertEquals(ackTable.min(), window * (sortedSet.first() / window));
      ackTable.ack(pair.first, pair.second);
      sortedSet.remove(pair.first);
    });
  }

  @DataProvider
  public Object[][] performanceTestProvider() {
    final long startTs = 0;
    final long window = 10;
    final long windowsCount = 4 * 1000;
    final long items = 10 * 1000;
    return new Object[][]{
            {
                    startTs,
                    window,
                    windowsCount,
                    Stream.generate(() -> ThreadLocalRandom.current().nextLong(1, window * windowsCount))
                            .distinct()
                            .limit(items)
                            .map(ts -> new Pair<>(ts, ThreadLocalRandom.current().nextLong()))
                            .collect(Collectors.toList())
            }
    };
  }

  @Test(dataProvider = "performanceTestProvider", invocationCount = 100, enabled = false)
  public void arrayAckTablePerformanceTest(long startTs, long window, long windowsCount, List<Pair<Long, Long>> xors) {
    final AckTable ackTable = new ArrayAckTable(startTs, window * windowsCount, window);
    performanceTest(ackTable, window, windowsCount, xors);
  }

  @Test(dataProvider = "performanceTestProvider", invocationCount = 100, enabled = false)
  public void treeAckTablePerformanceTest(long startTs, long window, long windowsCount, List<Pair<Long, Long>> xors) {
    final AckTable ackTable = new TreeAckTable(startTs, window);
    performanceTest(ackTable, window, windowsCount, xors);
  }

  private void performanceTest(AckTable ackTable, long window, long windowsCount, List<Pair<Long, Long>> xors) {
    final long start = System.nanoTime();
    LongStream.range(0, windowsCount).forEach(value -> ackTable.report(window * value, 0));
    xors.forEach(pair -> ackTable.ack(pair.first, pair.second));
    Collections.shuffle(xors);
    xors.forEach(pair -> ackTable.ack(pair.first, pair.second));
    System.out.println(System.nanoTime() - start);
  }
}
