package com.spbsu.datastream.core.test;

import java.util.function.Consumer;

public final class PrintlnConsumer implements Consumer<String> {
  @Override
  public void accept(final String value) {
    System.out.println(value);
  }
}
