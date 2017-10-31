package com.spbsu.flamestream.core.data.meta.impl;

import com.spbsu.flamestream.core.FakeAtomicHandle;
import com.spbsu.flamestream.core.HashFunction;
import com.spbsu.flamestream.core.data.DataItem;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.core.data.meta.Meta;
import com.spbsu.flamestream.core.graph.AtomicHandle;
import com.spbsu.flamestream.core.graph.ops.FlatMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 01.07.2017
 */
public class MetaFlatMapTest {
  @Test
  public void testFlatFilterLogic() {
    final int flatNumber = 10;
    final Function<Integer, Stream<Integer>> function = integer -> Stream.generate(() -> integer).limit(flatNumber);
    final FlatMap<Integer, Integer> flatMap = new FlatMap<>(function, HashFunction.constantHash(1));

    final int inputSize = 10;
    final List<DataItem<Integer>> input = IntStream.range(0, inputSize)
            .mapToObj(i -> new PayloadDataItem<>(Meta.meta(new GlobalTime(i, 1)), i))
            .collect(Collectors.toList());

    final List<DataItem<Integer>> out = new ArrayList<>();
    //noinspection unchecked
    final AtomicHandle handle = new FakeAtomicHandle((port, di) -> out.add((DataItem<Integer>) di));
    flatMap.onStart(handle);
    input.forEach(in -> flatMap.onPush(flatMap.inPort(), in, handle));

    for (int i = 0; i < inputSize; i++) {
      for (int j = 0; j < flatNumber; j++) {
        Assert.assertEquals(i, out.get(i * flatNumber + j).payload().intValue());
        final MetaImpl meta = (MetaImpl) out.get(i * flatNumber + j).meta();
        final TraceImpl trace = (TraceImpl) meta.trace();
        Assert.assertEquals(i + 1, LocalEvent.localTimeOf(trace.trace[0]));
        Assert.assertEquals(j, LocalEvent.childIdOf(trace.trace[0]));
      }
    }
  }
}
