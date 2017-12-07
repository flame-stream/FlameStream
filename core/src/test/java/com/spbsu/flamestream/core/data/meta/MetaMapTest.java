package com.spbsu.flamestream.core.data.meta;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.FlameStreamSuite;
import com.spbsu.flamestream.core.data.PayloadDataItem;
import com.spbsu.flamestream.core.graph.FlameMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * User: Artem
 * Date: 01.07.2017
 */
public class MetaMapTest extends FlameStreamSuite {
  @Test
  public void testFlatFilterLogic() {
    final int flatNumber = 10;
    final Function<Integer, Stream<Integer>> function = integer -> Stream.generate(() -> integer).limit(flatNumber);
    final FlameMap<Integer, Integer> map = new FlameMap<>(function, Integer.class);

    final int inputSize = 10;
    final List<DataItem> input = IntStream.range(0, inputSize)
            .mapToObj(i -> new PayloadDataItem(Meta.meta(new GlobalTime(i, EdgeInstance.MIN)), i))
            .collect(Collectors.toList());
    //noinspection ConstantConditions
    final List<DataItem> out = input.stream()
            .flatMap(map.operation()::apply)
            .collect(Collectors.toList());

    for (int i = 0; i < inputSize; i++) {
      for (int j = 0; j < flatNumber; j++) {
        Assert.assertEquals(i, out.get(i * flatNumber + j).payload(Integer.class).intValue());
        final MetaImpl meta = (MetaImpl) out.get(i * flatNumber + j).meta();
        final TraceImpl trace = (TraceImpl) meta.trace();
        Assert.assertEquals(i + 1, LocalEvent.localTimeOf(trace.trace[0]));
        Assert.assertEquals(j, LocalEvent.childIdOf(trace.trace[0]));
      }
    }
  }
}
