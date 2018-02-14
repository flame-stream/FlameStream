package com.spbsu.flamestream.runtime.graph;

import com.spbsu.flamestream.core.DataItem;
import com.spbsu.flamestream.core.data.meta.Meta;

import java.util.concurrent.ThreadLocalRandom;

class BroadcastDataItem implements DataItem {
  private final DataItem inner;
  private final Meta newMeta;
  private final long xor;

  BroadcastDataItem(DataItem inner, Meta newMeta) {
    this.inner = inner;
    this.newMeta = newMeta;
    this.xor = ThreadLocalRandom.current().nextLong();
  }

  @Override
  public Meta meta() {
    return newMeta;
  }

  @Override
  public <T> T payload(Class<T> expectedClass) {
    return inner.payload(expectedClass);
  }

  @Override
  public long xor() {
    return xor;
  }
}
