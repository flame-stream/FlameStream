package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.graph.FlameMap;
import java.util.stream.Stream;

public interface MapOp<Input, Output> extends FlameMap.SerializableFunction<Input, Stream<Output>> {
  Class<Input> inputClass();
}
