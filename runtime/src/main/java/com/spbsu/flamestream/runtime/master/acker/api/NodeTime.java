package com.spbsu.flamestream.runtime.master.acker.api;

public class NodeTime {
  public final String jobaId;
  public final long time;

  public NodeTime(String jobaId, long time) {
    this.jobaId = jobaId;
    this.time = time;
  }
}
