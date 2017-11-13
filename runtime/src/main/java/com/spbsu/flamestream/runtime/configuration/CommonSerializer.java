package com.spbsu.flamestream.runtime.configuration;

import akka.actor.Props;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import org.objenesis.strategy.StdInstantiatorStrategy;

public final class CommonSerializer implements TickInfoSerializer, FrontSerializer {
  private final Kryo kryo;

  public CommonSerializer() {
    this.kryo = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  @Override
  public byte[] serialize(TickInfo tickInfo) {
    final ByteBufferOutput o = new ByteBufferOutput(1000, 20000);
    kryo.writeObject(o, tickInfo);
    return o.toBytes();
  }

  @Override
  public TickInfo deserializeTick(byte[] data) {
    final ByteBufferInput input = new ByteBufferInput(data);
    return kryo.readObject(input, TickInfo.class);
  }

  @Override
  public byte[] serialize(Props props) {
    final ByteBufferOutput o = new ByteBufferOutput(1000, 20000);
    kryo.writeObject(o, props);
    return o.toBytes();
  }

  @Override
  public Props deserializeFront(byte[] date) {
    final ByteBufferInput input = new ByteBufferInput(date);
    return kryo.readObject(input, Props.class);
  }
}
