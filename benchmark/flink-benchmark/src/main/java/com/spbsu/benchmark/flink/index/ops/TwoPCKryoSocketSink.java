package com.spbsu.benchmark.flink.index.ops;

import com.spbsu.benchmark.flink.index.Result;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.ArrayList;
import java.util.List;

public class TwoPCKryoSocketSink extends TwoPhaseCommitSinkFunction<Result, List<Result>, Object> {
  private static final long serialVersionUID = 1L;
  private final KryoSocketSink socketSink;

  public TwoPCKryoSocketSink(String hostName, int port, ExecutionConfig config) {
    super(
            new ListTypeInfo<>(Result.class).createSerializer(config),
            new GenericTypeInfo<>(Object.class).createSerializer(config)
    );
    socketSink = new KryoSocketSink(hostName, port);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    socketSink.open(parameters);
  }

  @Override
  protected void invoke(List<Result> transaction, Result value, Context context) {
    transaction.add(value);
  }

  @Override
  protected List<Result> beginTransaction() {
    return new ArrayList<>();
  }

  @Override
  protected void preCommit(List<Result> transaction) {
  }

  @Override
  protected void commit(List<Result> transaction) {
    transaction.forEach(result -> socketSink.invoke(result, null));
  }

  @Override
  protected void abort(List<Result> transaction) {
  }

  @Override
  public void close() {
    socketSink.close();
  }
}

