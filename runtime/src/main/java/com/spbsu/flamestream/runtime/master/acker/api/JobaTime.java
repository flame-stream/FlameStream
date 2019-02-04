package com.spbsu.flamestream.runtime.master.acker.api;

import com.spbsu.flamestream.runtime.graph.Joba;

public class JobaTime {
  public final Joba.Id jobaId;
  public final long time;

  public JobaTime(Joba.Id jobaId, long time) {
    this.jobaId = jobaId;
    this.time = time;
  }
}
