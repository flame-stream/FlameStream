package com.spbsu.flamestream.runtime.acker;

import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.runtime.acker.table.AckTable;
import com.spbsu.flamestream.runtime.acker.table.ArrayAckTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Random;
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
    Assert.assertEquals(table.tryPromote(0), 0);
  }

  @Test
  public void singleHeartbeatTest() {
    final long heartbeat = (long) 1e7;
    final AckTable table = new ArrayAckTable(0, 10, 10);
    Assert.assertEquals(table.tryPromote(heartbeat), heartbeat);
  }

  @Test
  public void singleAckTest() {
    final long ack = 70;
    final AckTable table = new ArrayAckTable(0, 14, 10);
    table.ack(ack, 1);
    Assert.assertEquals(table.tryPromote(Long.MAX_VALUE), ack);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void overflowTest() {
    final AckTable table = new ArrayAckTable(0, 10, 10);
    table.ack((long) 1e9, 1);
  }

  @Test
  public void logicTest() {
    final Random rd = new Random(1);
    final long head = 0;
    final int capacity = 100;
    final int window = 10;
    final long width = capacity * window;

    final AckTable table = new ArrayAckTable(head, capacity, window);
    for (long epoch = 0; epoch < 10000; ++epoch) {
      final long epochFloor = head + epoch * width / 2;
      final long epochCeil = epochFloor + width / 2;

      final Map<Long, Long> xors = rd
              .longs(epochFloor, epochCeil)
              .limit(width)
              .distinct()
              .boxed()
              .collect(Collectors.toMap(Function.identity(), e -> rd.nextLong()));

      xors.forEach(table::ack);
      table.tryPromote(epochCeil);
      xors.forEach(table::ack);

      Assert.assertEquals(table.tryPromote(epochCeil), epochCeil);
    }
  }
}
