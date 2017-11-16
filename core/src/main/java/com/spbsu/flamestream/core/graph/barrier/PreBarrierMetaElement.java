package com.spbsu.flamestream.core.graph.barrier;

import com.spbsu.flamestream.core.graph.HashFunction;

class PreBarrierMetaElement<T> {
  @SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
  static final HashFunction<PreBarrierMetaElement<?>> HASH_FUNCTION = new HashFunction<PreBarrierMetaElement<?>>() {
    @Override
    public int hash(PreBarrierMetaElement<?> element) {return element.metaHash();}
  };

  private final T payload;
  private final int metaHash;

  PreBarrierMetaElement(T payload, int metaHash) {
    this.payload = payload;
    this.metaHash = metaHash;
  }

  T payload() {
    return payload;
  }

  private int metaHash() {
    return metaHash;
  }

  @Override
  public String toString() {
    return "PreBarrierMetaElement{" + "payload=" + payload + ", metaHash=" + metaHash + '}';
  }
}
