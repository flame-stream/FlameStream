package com.spbsu.flamestream.runtime.master.acker;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class LocalAckerTest {
  @Test
  public void testPartitionTimeCeil() {
    final LocalAcker.Partitions partitions = new LocalAcker.Partitions(2);
    assertEquals(partitions.partitionTime(0, 0, 1), 2);
    assertEquals(partitions.partitionTime(1, 0, 1), 1);
    assertEquals(partitions.partitionTime(1, 0, Long.MIN_VALUE), Long.MIN_VALUE + 1);
    assertEquals(partitions.partitionTime(1, 0, Long.MIN_VALUE + 1), Long.MIN_VALUE + 1);
    assertEquals(partitions.partitionTime(1, 0, Long.MIN_VALUE + 2), Long.MIN_VALUE + 3);
    testPartitionTimeCeilOfTimePartition(1, 0);
    testPartitionTimeCeilOfTimePartition(Integer.MIN_VALUE, 0);
    testPartitionTimeCeilOfTimePartition(Integer.MIN_VALUE, Long.MIN_VALUE);
    testPartitionTimeCeilOfTimePartition(Integer.MAX_VALUE, 0);
  }

  public void testPartitionTimeCeilOfTimePartition(int hash, long time) {
    final LocalAcker.Partitions partitions = new LocalAcker.Partitions(2);
    assertEquals(partitions.partitionTime(partitions.timePartition(hash, time), hash, time), time);
    assertEquals(partitions.partitionTime(1 - partitions.timePartition(hash, time), hash, time), time + 1);
  }

  @Test
  public void testTimePartition() {
    final LocalAcker.Partitions partitions = new LocalAcker.Partitions(3);
    assertEquals(partitions.timePartition(0, 0), 0);
    assertEquals(partitions.timePartition(0, -1), 2);
    assertEquals(partitions.timePartition(0, -2), 1);
    assertEquals(partitions.timePartition(0, 0), 0);
    assertEquals(partitions.timePartition(0, 1), 1);
  }
}
