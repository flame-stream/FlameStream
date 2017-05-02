package com.spbsu.datastream.core;

import java.util.function.Function;

final class MyFunc implements Function<String, String> {
  @Override
  public String apply(final String integer) {
    return integer + "; filter was here!";
  }
}
