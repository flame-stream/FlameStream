package com.spbsu.datastream.core.buffer.impl;

import com.spbsu.datastream.core.buffer.LongBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

/**
 * User: Artem
 * Date: 31.08.2017
 */
public class LongRingBufferTest {

  @Test
  public void correctnessTest() {
    final int capacity = 1000000;
    final LongBuffer longBuffer = new LongRingBuffer(capacity);

    final Deque<Long> testDeque = new ArrayDeque<>();
    final int iterations = capacity - 1;
    LongStream.range(0, iterations)
            .map(operand -> ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE))
            .forEach(value -> {
              if (value % 5 == 0 && !longBuffer.isEmpty()) {
                longBuffer.removeFirst();
                testDeque.removeFirst();
              } else {
                longBuffer.addLast(value);
                testDeque.addLast(value);
              }
            });

    Assert.assertEquals(testDeque.size(), longBuffer.size());
    IntStream.range(0, longBuffer.size()).forEach(pos -> Assert.assertTrue(longBuffer.get(pos) == testDeque.pollFirst()));
  }
}
