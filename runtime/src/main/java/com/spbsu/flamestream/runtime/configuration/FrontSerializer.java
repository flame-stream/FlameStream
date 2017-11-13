package com.spbsu.flamestream.runtime.configuration;

import akka.actor.Props;

public interface FrontSerializer {
  byte[] serialize(Props props);

  Props deserializeFront(byte[] date);
}
