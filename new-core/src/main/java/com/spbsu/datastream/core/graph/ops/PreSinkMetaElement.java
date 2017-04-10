package com.spbsu.datastream.core.graph.ops;

import com.spbsu.datastream.core.HashFunction;

public class PreSinkMetaElement<T> {
  public final static HashFunction<PreSinkMetaElement> HASH_FUNCTION = new HashFunction<PreSinkMetaElement>() {
    @Override
    public boolean equal(final PreSinkMetaElement o1, final PreSinkMetaElement o2) {
      return o1.metaHash() == o2.metaHash();
    }

    @Override
    public int hash(final PreSinkMetaElement value) {
      return value.metaHash();
    }
  };

  private final T payload;

  private final int metaHash;

  public PreSinkMetaElement(final T payload, final int metaHash) {
    this.payload = payload;
    this.metaHash = metaHash;
  }

  public T payload() {
    return payload;
  }

  public int metaHash() {
    return metaHash;
  }
}
