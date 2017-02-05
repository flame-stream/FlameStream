package com.spbsu.datastream.core.serialization;

import java.nio.ByteBuffer;

/**
 * Created by marnikitta on 2/4/17.
 * copypasted from akka
 */
interface ByteBufferSerializer {
  /**
   * Serializes the given object into the `ByteBuffer`.
   */
  void toBinary(Object o, ByteBuffer buf);

  /**
   * Produces an object from a `ByteBuffer`, with an optional type-hint;
   * the class should be loaded using ActorSystem.dynamicAccess.
   */
  Object fromBinary(ByteBuffer buf, Class<?> hint);
}
