package com.spbsu.flamestream.core;

public class OutputPayload {
  private final DataItem dataItem;
  public final long notificationAwaitTime;

  public OutputPayload(DataItem dataItem, long notificationAwaitTime) {
    this.dataItem = dataItem;
    this.notificationAwaitTime = notificationAwaitTime;
  }

  public <T> T payload(Class<T> tClass) {
    return dataItem.payload(tClass);
  }
}
