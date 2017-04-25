package com.spbsu.datastream.core.feedback;

import com.spbsu.datastream.core.GlobalTime;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class AckLedgerImpl implements AckLedger {
  private static final Comparator<Map.Entry<Integer, AckTable>> COMPARATOR = Comparator
          .comparingLong((Map.Entry<Integer, AckTable> e) -> e.getValue().min())
          .thenComparingInt(Map.Entry::getKey);
  private final long startTs;

  private final long window;

  private final Map<Integer, AckTable> tables;

  public AckLedgerImpl(final long startTime, final long window, final Collection<Integer> initHashes) {
    this.startTs = startTime;
    this.window = window;
    this.tables = initHashes.stream()
            .collect(Collectors.toMap(Function.identity(), h -> new AckTableImpl(startTime, window)));
  }

  @Override
  public void report(final GlobalTime windowHead) {
    this.tables.get(windowHead.initHash()).report(windowHead.time());
  }

  @Override
  public GlobalTime min() {
    return this.tables.entrySet().stream()
            .min(AckLedgerImpl.COMPARATOR)
            .map(e -> new GlobalTime(e.getValue().min(), e.getKey()))
            .orElseThrow(IllegalStateException::new);
  }

  @Override
  public void ack(final GlobalTime windowHead, final long xor) {
    this.tables.get(windowHead.initHash()).ack(windowHead.time(), xor);
  }

  @Override
  public Collection<Integer> initHashes() {
    return this.tables.keySet();
  }

  @Override
  public long startTs() {
    return this.startTs;
  }

  @Override
  public long window() {
    return this.window;
  }
}
