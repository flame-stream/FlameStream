package com.spbsu.datastream.core.raw;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * User: Artem
 * Date: 19.09.2017
 */
public class BatchRawData<T> implements RawData<T> {
  private final List<T> batch;

  public BatchRawData(Collection<T> origin) {
    batch = new ArrayList<>(origin);
  }

  @Override
  public Iterator<T> iterator() {
    return batch.iterator();
  }
}
