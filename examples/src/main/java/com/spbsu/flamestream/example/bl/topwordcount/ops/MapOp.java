package com.spbsu.flamestream.example.bl.topwordcount.ops;

import java.util.function.Function;
import java.util.stream.Stream;

public interface MapOp<Input, Output> extends Function<Input, Stream<Output>> {
  Class<Input> inputClass();
}
