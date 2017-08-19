package com.spbsu.datastream.benchmarks.measure;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

/**
 * User: Artem
 * Date: 17.08.2017
 */
public class LatencyMeasurer<T> {
  private final TObjectLongMap<T> starts = new TObjectLongHashMap<>();
  private final TObjectLongMap<T> latencies = new TObjectLongHashMap<>();
  private final LatencyMeasurerDelegate<T> delegate;

  private final long measurePeriod;
  private long delay;

  public LatencyMeasurer(LatencyMeasurerDelegate<T> delegate, long warmUpDelay, long measurePeriod) {
    this.delegate = delegate;
    this.measurePeriod = measurePeriod;
    this.delay = warmUpDelay;
  }

  public void start(T key) {
    delegate.onStart(key);
    if (--delay > 0)
      return;
    delay = measurePeriod;

    final long startTs = System.nanoTime();
    starts.put(key, startTs);
    delegate.onProcess(key);
  }

  public void finish(T key) {
    if (starts.containsKey(key)) {
      final long latency = System.nanoTime() - starts.get(key);
      latencies.put(key, latency);
      delegate.onFinish(key, latency);
    }
  }

  public void stopMeasure() {
    delegate.onStopMeasure();
  }

  public long[] latencies() {
    return latencies.values();
  }
}
