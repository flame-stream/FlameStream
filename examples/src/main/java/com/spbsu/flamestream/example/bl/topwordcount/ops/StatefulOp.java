package com.spbsu.flamestream.example.bl.topwordcount.ops;

import com.spbsu.flamestream.core.Equalz;
import com.spbsu.flamestream.core.HashFunction;

public interface StatefulOp<Input, Output extends Input> {
  int groupingHash(Input input);

  default HashFunction groupingHashFunction(HashFunction hashFunction) {
    return hashFunction;
  }

  boolean groupingEquals(Input left, Input right);

  default Equalz groupingEqualz(Equalz equalz) {
    return equalz;
  }

  Output output(Input input);

  Output reduce(Output left, Output right);

  Class<Input> inputClass();

  Class<Output> outputClass();
}
