package com.spbsu.datastream.core.meta;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.FakeAtomicHandle;
import com.spbsu.datastream.core.HashFunction;
import com.spbsu.datastream.core.PayloadDataItem;
import com.spbsu.datastream.core.graph.ops.FlatMap;
import com.spbsu.datastream.core.range.atomic.AtomicHandle;
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
public class FlatMapTest {
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
