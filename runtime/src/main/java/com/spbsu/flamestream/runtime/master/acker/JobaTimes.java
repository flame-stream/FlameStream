package com.spbsu.flamestream.runtime.master.acker;

import com.spbsu.flamestream.runtime.graph.Joba;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class JobaTimes {
  private final HashMap<Joba.Id, Long> all;

  JobaTimes() {
    this.all = new HashMap<>();
  }

  private JobaTimes(HashMap<Joba.Id, Long> all) {
    this.all = all;
  }

  public boolean greaterThanOrNotComparableTo(@NotNull JobaTimes other) {
    for (Joba.Id id : all.keySet()) {
      if (all.getOrDefault(id, Long.MIN_VALUE) > other.all.getOrDefault(id, Long.MIN_VALUE)) {
        return true;
      }
    }
    return false;
  }

  public JobaTimes updated(Joba.Id id, long time) {
    final HashMap<Joba.Id, Long> all = new HashMap<>(this.all);
    final long currentTime = all.getOrDefault(id, Long.MIN_VALUE);
    if (currentTime > time)
      throw new RuntimeException("joba time is decreasing from " + currentTime + " to " + time);
    all.put(id, time);
    return new JobaTimes(all);
  }

  public JobaTimes min(JobaTimes other) {
    final Set<Joba.Id> keys = new HashSet<>(all.keySet());
    keys.addAll(other.all.keySet());
    final HashMap<Joba.Id, Long> min = new HashMap<>(all);
    for (Joba.Id id : keys) {
      min.put(id, Long.min(all.getOrDefault(id, Long.MIN_VALUE), other.all.getOrDefault(id, Long.MIN_VALUE)));
    }
    return new JobaTimes(min);
  }

  public JobaTimes max(JobaTimes other) {
    HashMap<Joba.Id, Long> max = new HashMap<>(this.all);
    for (Joba.Id id : other.all.keySet()) {
      if (all.getOrDefault(id, Long.MIN_VALUE) < other.all.getOrDefault(id, Long.MIN_VALUE))
        max.put(id, other.all.get(id));
    }
    return new JobaTimes(max);
  }
}
