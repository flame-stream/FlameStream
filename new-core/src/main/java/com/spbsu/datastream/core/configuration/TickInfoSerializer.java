package com.spbsu.datastream.core.configuration;

import com.spbsu.datastream.core.tick.TickInfo;

public interface TickInfoSerializer {
  byte[] serialize(TickInfo tickInfo);

  TickInfo deserialize(byte[] date);

}
