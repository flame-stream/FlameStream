package com.spbsu.flamestream.example.bl.topwordcount.ops;

public interface StatefulOp<Input, Output extends Input> {
  Output output(Input input);

  Output reduce(Output left, Output right);

  Class<Input> inputClass();

  Class<Output> outputClass();
}
