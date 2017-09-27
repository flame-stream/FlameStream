package com.spbsu.flamestream.runtime.raw;

import java.util.Iterator;

/**
 * User: Artem
 * Date: 19.09.2017
 */
public class SingleRawData<T> implements RawData<T> {
  private final T payload;

  public SingleRawData(T payload) {
    this.payload = payload;
  }

  public T payload() {
    return payload;
  }

  @Override
  public String toString() {
    return "RawData{" + "payload=" + payload +
            '}';
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      private boolean done = false;

      @Override
      public boolean hasNext() {
        return !done;
      }

      @Override
      public T next() {
        done = true;
        return payload;
      }
    };
  }
}
