package com.spbsu.flamestream.runtime;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntConsumer;

public class ConfigureFromEnv {
  public static void configureFromEnv(String key, IntConsumer intConsumer) {
    if (System.getenv().containsKey(key)) {
      intConsumer.accept(Integer.parseInt(System.getenv(key)));
    }
  }

  public static void configureFromEnv(String key, Consumer<String> consumer) {
    if (System.getenv().containsKey(key)) {
      consumer.accept(System.getenv(key));
    }
  }

  public static <T> void configureFromEnv(String key, Function<String, T> factory, Consumer<T> consumer) {
    if (System.getenv().containsKey(key)) {
      consumer.accept(factory.apply(System.getenv(key)));
    }
  }
}
