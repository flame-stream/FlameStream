package com.spbsu.datastream.core.configuration;

import com.spbsu.datastream.core.node.TickInfo;

public interface TickInfoSerializer {
  byte[] serialize(TickInfo tickInfo);

  TickInfo deserialize(byte[] date);

}
