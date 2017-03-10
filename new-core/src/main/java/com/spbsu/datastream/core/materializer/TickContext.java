package com.spbsu.datastream.core.materializer;

import com.spbsu.datastream.core.materializer.atomic.AtomicHandle;
import com.spbsu.datastream.core.materializer.locator.LocalPortLocator;

public interface TickContext {
  LocalPortLocator localLocator();

  AtomicHandle handle();
}
