package com.spbsu.datastream.core.barrier;

import com.spbsu.datastream.core.HashFunction;

import java.util.Objects;

final class PreSinkMetaElement<T> {
  static final HashFunction<PreSinkMetaElement<?>> HASH_FUNCTION = new HashFunction<PreSinkMetaElement<?>>() {
    @Override
    public boolean equal(PreSinkMetaElement<?> o1, PreSinkMetaElement<?> o2) {
      return Objects.equals(o1.metaHash(), o2.metaHash());
    }

    @Override
    public int hash(PreSinkMetaElement<?> value) {
      return value.metaHash();
    }
  };

  private final T payload;

  private final int metaHash;

  PreSinkMetaElement(T payload, int metaHash) {
    this.payload = payload;
    this.metaHash = metaHash;
  }

  T payload() {
    return this.payload;
  }

  int metaHash() {
    return this.metaHash;
  }


  @Override
  public String toString() {
    return "PreSinkMetaElement{" + "payload=" + this.payload +
            ", metaHash=" + this.metaHash +
            '}';
  }
}
