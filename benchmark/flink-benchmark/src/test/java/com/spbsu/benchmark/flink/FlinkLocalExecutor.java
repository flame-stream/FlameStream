package com.spbsu.benchmark.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.function.Consumer;

/**
 * User: Artem
 * Date: 05.10.2017
 */
class FlinkLocalExecutor {
  //dirty code for avoiding serialization
  private static Consumer<Object> consumer;

  private final StreamExecutionEnvironment executionEnvironment;

  FlinkLocalExecutor(int bufferTimeout) {
    executionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1);
    executionEnvironment.setBufferTimeout(bufferTimeout);
  }

  <T> void execute(FlinkStream flinkStream, SourceFunction<T> source, Consumer<Object> output) {
    consumer = output;
    //noinspection unchecked
    flinkStream.stream(executionEnvironment.addSource(source)).addSink(value -> consumer.accept(value));
    try {
      executionEnvironment.execute();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
