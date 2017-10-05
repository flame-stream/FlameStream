package com.spbsu.benchmark.flink;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * User: Artem
 * Date: 05.10.2017
 */
public interface FlinkStream<T, R> {
  DataStream<R> stream(DataStream<T> source);
}
