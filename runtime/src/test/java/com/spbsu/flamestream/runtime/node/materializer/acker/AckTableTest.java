package com.spbsu.flamestream.runtime.node.materializer.acker;

import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.runtime.node.materializer.acker.table.AckTable;
import com.spbsu.flamestream.runtime.node.materializer.acker.table.ArrayAckTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * User: Artem
 * Date: 30.08.2017
 */
public class AckTableTest extends FlameStreamSuite {
  private static final Logger LOG = LoggerFactory.getLogger(AckTableTest.class);

  @Test
  public void emptyTableTest() {
    final AckTable table = new ArrayAckTable(0, 10, 10);
    Assert.assertEquals(table.min(), Long.MIN_VALUE);
  }

  @Test
  public void singleHeartbeatTest() {
    final long heartbeat = (long) 1e7;
    final AckTable table = new ArrayAckTable(0, 10, 10);
    table.heartbeat(heartbeat);
    Assert.assertEquals(table.min(), heartbeat);
  }

  @Test
  public void singleAckTest() {
    final long ack = 70;
    final AckTable table = new ArrayAckTable(0, 10, 10);
    table.ack(ack, 1);
    table.heartbeat(Long.MAX_VALUE);
    Assert.assertEquals(table.min(), ack);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void overflowTest() {
    final AckTable table = new ArrayAckTable(0, 10, 10);
    table.ack((long) 1e9, 1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void notMonotonicHeartbeatsTest() {
    final AckTable table = new ArrayAckTable(0, 10, 10);
    table.heartbeat(2);
    table.heartbeat(1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void ackAfterCollapseTest() {
    final AckTable table = new ArrayAckTable(0, 10, 10);
    table.heartbeat(Long.MAX_VALUE);
    table.ack(4, 1);
  }

  @Test
  public void logicTest() {
    final long head = 1;
    final int capacity = 100;
    final long window = 10;
    final long width = capacity * window;

    final AckTable table = new ArrayAckTable(head, capacity, window);

    for (long epoch = 0; epoch < 10000; ++epoch) {
      final long epochFloor = head + epoch * width;
      final long epochCeil = head + (epoch + 1) * width;

      final Map<Long, Long> xors = ThreadLocalRandom.current()
              .longs(epochFloor, epochCeil)
              .limit(width)
              .distinct()
              .boxed()
              .collect(Collectors.toMap(Function.identity(), e -> ThreadLocalRandom.current().nextLong()));

      xors.forEach(table::ack);
      table.heartbeat(epochCeil);
      xors.forEach((ts, xor) -> {
        table.ack(ts, xor);
        Assert.assertTrue(table.min() <= epochCeil);
      });

      Assert.assertEquals(table.min(), epochCeil);
    }
  }
}
