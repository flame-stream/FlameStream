package com.spbsu.flamestream.example.bl.topwordcount.ops;

public interface Hashing<Input> {
  default int hash(Input input) {
    return 0;
  }

  default boolean equals(Input left, Input right) {
    return true;
  }
}
