package com.spbsu.flamestream.core.data.invalidation;

import java.util.ArrayList;
import java.util.Collections;

public class SynchronizedArrayInvalidatingBucket extends ArrayInvalidatingBucket {
  public SynchronizedArrayInvalidatingBucket() {
    super(Collections.synchronizedList(new ArrayList<>()));
  }
}
