package com.spbsu.datastream.core.buffer.impl;

import com.spbsu.datastream.core.buffer.LongBuffer;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import org.testng.Assert;
import org.testng.annotations.Test;

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
    final TLongList testList = new TLongArrayList(capacity);
    LongStream.range(0, capacity).forEach(value -> testList.add(0L));

    final int iterations = 10000;
    final int[] upperRandomBound = {capacity};
    LongStream.generate(() -> ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE))
            .limit(iterations)
            .forEach(value -> {
              if (value % 5 == 0 && !longBuffer.isEmpty()) {
                longBuffer.removeFirst();
                testList.removeAt(0);
                upperRandomBound[0]--;
              } else {
                final int index = ThreadLocalRandom.current().nextInt(0, upperRandomBound[0]);
                longBuffer.put(index, value);
                testList.set(index, value);
              }
            });
    IntStream.range(0, longBuffer.size()).forEach(pos -> Assert.assertEquals(longBuffer.get(pos), testList.get(pos)));
  }
}
