package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.graph.SerializableFunction;

import java.util.stream.Stream;

public interface MapOp<Input, Output> extends SerializableFunction<Input, Stream<Output>> {
  Class<Input> inputClass();
}
