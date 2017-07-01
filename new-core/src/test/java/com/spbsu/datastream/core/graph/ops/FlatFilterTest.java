package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.*;
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
public class FlatFilterTest {
  @Test
  public void testFlatFilterLogic() {
    final int flatNumber = 10;
    final Function<Integer, Stream<Integer>> function = integer -> Stream.generate(() -> integer).limit(flatNumber);
    final FlatFilter<Integer, Integer> flatFilter = new FlatFilter<>(function, HashFunction.constantHash(1));

    final int inputSize = 10;
    final List<DataItem<Integer>> input = IntStream.range(0, inputSize)
            .mapToObj(i -> new PayloadDataItem<>(new Meta(new GlobalTime(i, 1)), i))
            .collect(Collectors.toList());

    final List<DataItem<Integer>> out = new ArrayList<>();
    //noinspection unchecked
    final AtomicHandle handle = new FakeAtomicHandle((port, di) -> out.add((DataItem<Integer>) di));
    flatFilter.onStart(handle);
    input.forEach(in -> flatFilter.onPush(flatFilter.inPort(), in, handle));

    for (int i = 0; i < inputSize; i++) {
      for (int j = 0; j < flatNumber; j++) {
        Assert.assertEquals(i, out.get(i * flatNumber + j).payload().intValue());
        Assert.assertEquals(i + 1, out.get(i * flatNumber + j).meta().trace().eventAt(0).localTime());
        Assert.assertEquals(j, out.get(i * flatNumber + j).meta().trace().eventAt(0).childId());
      }
    }
  }
}
