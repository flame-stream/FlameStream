package com.spbsu.flamestream.runtime.application;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.runtime.DumbInetSocketAddress;
import com.spbsu.flamestream.runtime.node.LifecycleWatcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

public final class WorkerApplication {
  public static final int PORT = 4387;
  private static final Logger LOG = LoggerFactory.getLogger(WorkerApplication.class);
  private final DumbInetSocketAddress host;
  private final String zkConnectString;
  private final int id;

  private ActorSystem system;

  public WorkerApplication(int id, String zkConnectString) throws UnknownHostException {
    this.id = id;
    this.host = new DumbInetSocketAddress("localhost", PORT);
    this.zkConnectString = zkConnectString;
  }

  public WorkerApplication(int id, DumbInetSocketAddress host, String zkConnectString) {
    this.id = id;
    this.host = host;
    this.zkConnectString = zkConnectString;
  }

  public static void main(String... args) throws IOException {
    final Config config;
    if (args.length == 1) {
      config = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[0])))
        .withFallback(ConfigFactory.load("fs"));
    } else {
      config = ConfigFactory.load("fs");
    }

    final int port = config.getInt("port");
    final String host = config.getString("host");
    final DumbInetSocketAddress socketAddress = new DumbInetSocketAddress(host, port);

    new WorkerApplication(config.getInt("id"), socketAddress, config.getString("zk_string")).run();
  }

  public void run() {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + host.port())
      .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + host.host()))
      .withFallback(ConfigFactory.load("remote"));
    this.system = ActorSystem.create("worker", config);

    system.actorOf(LifecycleWatcher.props(zkConnectString, id), "watcher");
  }

  public void shutdown() {
    try {
      Await.ready(system.terminate(), Duration.Inf());
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
