package com.spbsu.datastream.core.ack.impl;

import com.spbsu.commons.util.Pair;
import com.spbsu.commons.util.logging.Interval;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 30.08.2017
 */
public class AckTableTest {

  @DataProvider
  public Object[][] logicAndPerformanceTestProvider() {
    return new Object[][]{
            {10, 40000, 10000},
            {10, 40000, 100000},
            {10, 1000000, 10000},
            {10, 1000000, 1000000}
    };
  }

  @Test(dataProvider = "logicAndPerformanceTestProvider")
  public void logicAndPerformanceTest(int window, int windowsCount, int range) {
    final AckTable ackTable = new AckTable(0, window * windowsCount, window);
    final List<Pair<Long, Long>> xors = Stream.generate(() -> ThreadLocalRandom.current().nextLong(1, window * windowsCount))
            .distinct()
            .limit(range)
            .map(ts -> new Pair<>(ts, ThreadLocalRandom.current().nextLong()))
            .collect(Collectors.toList());
    final SortedSet<Long> sortedSet = new TreeSet<>();

    Interval.start();
    IntStream.range(0, windowsCount).forEach(value -> ackTable.report(window * value, 0));
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
    Interval.stopAndPrint();
  }
}
