package com.spbsu.datastream.core.test;

import com.spbsu.datastream.core.application.WorkerApplication;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class StartWorker1 {
  public static void main(final String... args) throws IOException {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7001);
    new WorkerApplication().run(address);
  }
}
