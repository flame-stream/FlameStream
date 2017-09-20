package com.spbsu.datastream.core.configuration;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.spbsu.datastream.core.tick.TickInfo;
import org.objenesis.strategy.StdInstantiatorStrategy;

public final class KryoInfoSerializer implements TickInfoSerializer {
  private final Kryo kryo;

  public KryoInfoSerializer() {
    this.kryo = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy()).setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  @Override
  public byte[] serialize(TickInfo tickInfo) {
    final ByteBufferOutput o = new ByteBufferOutput(1000, 20000);
    kryo.writeObject(o, tickInfo);
    return o.toBytes();
  }

  @Override
  public TickInfo deserialize(byte[] date) {
    final ByteBufferInput input = new ByteBufferInput(date);
    return kryo.readObject(input, TickInfo.class);
  }
}
