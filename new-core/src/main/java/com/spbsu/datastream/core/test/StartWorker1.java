package com.spbsu.datastream.core.test;

import com.spbsu.datastream.core.application.WorkerApplication;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public final class StartWorker1 {
  private StartWorker1() {
  }

  public static void main(final String... args) throws IOException {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 7001);
    new WorkerApplication().run(address, "localhost:2181");
  }
}
