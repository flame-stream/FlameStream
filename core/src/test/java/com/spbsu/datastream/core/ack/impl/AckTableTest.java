package com.spbsu.datastream.core.ack.impl;

import com.spbsu.commons.util.logging.Interval;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

/**
 * User: Artem
 * Date: 30.08.2017
 */
public class AckTableTest {

  @DataProvider
  public Object[][] logicAndPerformanceTestProvider() {
    return new Object[][]{
            {10, 1000000, 100000},
            {10, 1000000, 1000000},
            {10, 1000000, 5000000}
    };
  }

  @Test(dataProvider = "logicAndPerformanceTestProvider")
  public void logicAndPerformanceTest(int window, int windowsCount, int range) {
    Interval.start();
    final AckTable ackTable = new AckTable(0, window);
    IntStream.range(0, windowsCount).forEach(value -> ackTable.report(window * value, value + 1));
    IntStream.range(0, range).forEach(value -> {
      final long ts = ThreadLocalRandom.current().nextLong(0, window * windowsCount);
      final long xor = ThreadLocalRandom.current().nextLong();
      ackTable.ack(ts, xor);
      ackTable.ack(ts, xor);
    });
    IntStream.range(0, windowsCount).forEach(value -> {
      Assert.assertEquals(ackTable.min(), window * value);
      ackTable.ack(window * value, value + 1);
    });
    Interval.stopAndPrint();
  }
}
