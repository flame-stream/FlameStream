package com.spbsu.datastream.benchmarks;

/**
 * User: Artem
 * Date: 17.08.2017
 */
public interface LatencyMeasurerDelegate<T> {
  void onStart(T key);

  void onProcess(T key);

  void onFinish(T key, long latency);

  void onStopMeasure();
}
