package com.spbsu.flamestream.runtime.utils.serialization;

import akka.actor.Props;

public interface FrontSerializer {
  byte[] serialize(Props props);

  Props deserializeFront(byte[] date);
}
