package com.spbsu.flamestream.runtime.acker.table;

import gnu.trove.map.hash.TObjectLongHashMap;

public class CollectiveFrontTable {
  private final TObjectLongHashMap<String> heartbeats = new TObjectLongHashMap<>();
  private final AckTable ackTable;

  public CollectiveFrontTable(long headValue, int capacity, long window) {
    this.ackTable = new ArrayAckTable(headValue, capacity, window);
  }

  public boolean ack(long timestamp, long xor) {
    return ackTable.ack(timestamp, xor);
  }

  public void addNode(String nodeId, long initHeartbeat) {
    heartbeats.put(nodeId, initHeartbeat);
  }

  public void heartbeat(String nodeId, long timestamp) {
    final long prev = heartbeats.get(nodeId);
    heartbeats.put(nodeId, Math.max(prev, timestamp));
    ackTable.heartbeat(minHeartbeat());
  }

  private long minHeartbeat() {
    final long[] min = {Long.MAX_VALUE};
    heartbeats.forEachValue(v -> {
      min[0] = Math.min(min[0], v);
      return true;
    });

    return min[0];
  }

  public long min() {
    return ackTable.min();
  }

  public boolean containsNode(String nodeId) {
    return heartbeats.containsKey(nodeId);
  }
}
