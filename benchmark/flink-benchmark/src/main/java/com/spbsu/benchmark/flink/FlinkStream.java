package com.spbsu.benchmark.flink;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.function.Function;

/**
 * User: Artem
 * Date: 05.10.2017
 */
interface FlinkStream<T, R> extends Function<DataStream<T>, DataStream<R>> {
  @Override
  default DataStream<R> apply(DataStream<T> dataStream) {
    return stream(dataStream);
  }

  DataStream<R> stream(DataStream<T> source);
}
