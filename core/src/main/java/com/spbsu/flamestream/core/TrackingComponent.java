package com.spbsu.flamestream.core;


import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;

public class TrackingComponent implements Comparable<TrackingComponent> {
  public static final TrackingComponent SOURCE = new TrackingComponent(0, Collections.emptyList());
  public static final TrackingComponent MAX = new TrackingComponent(Integer.MAX_VALUE, Collections.emptyList());

  public final int index;
  public final List<TrackingComponent> adjacent;

  public TrackingComponent(int index, List<TrackingComponent> adjacent) {
    if (index < 0)
      throw new IllegalArgumentException(String.valueOf(index));
    for (final TrackingComponent trackingComponent : adjacent) {
      if (index <= trackingComponent.index) {
        throw new IllegalArgumentException(trackingComponent.toString());
      }
    }
    this.adjacent = Collections.unmodifiableList(adjacent);
    this.index = index;
  }

  @Override
  public int compareTo(@NotNull TrackingComponent o) {
    return Integer.compare(index, o.index);
  }
}
