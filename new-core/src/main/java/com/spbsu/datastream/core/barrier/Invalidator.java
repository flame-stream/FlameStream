package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.GlobalTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Invalidator {
  private final Map<GlobalTime, InvalidateBucket> buckets = new HashMap<>();

  private static final class InvalidateBucket {
    private final List<DataItem<?>> brothersAndSisters = new ArrayList<>();

    public InvalidateBucket() {
    }
  }
}
