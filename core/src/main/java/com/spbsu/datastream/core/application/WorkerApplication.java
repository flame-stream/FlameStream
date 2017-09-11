package com.spbsu.datastream.core.application;

import akka.actor.ActorSystem;
import com.spbsu.datastream.core.node.LifecycleWatcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

public final class WorkerApplication {
  private static final Logger LOG = LoggerFactory.getLogger(WorkerApplication.class);

  private final InetSocketAddress host;
  private final String zkConnectString;
  private final int id;

  private ActorSystem system;

  public WorkerApplication(int id, String zkConnectString) throws UnknownHostException {
    this.id = id;
    this.host = new InetSocketAddress(InetAddress.getLocalHost(), 4387);
    this.zkConnectString = zkConnectString;
  }

  public WorkerApplication(int id, InetSocketAddress host, String zkConnectString) {
    this.id = id;
    this.host = host;
    this.zkConnectString = zkConnectString;
  }

  public static void main(String... args) throws IOException {
    final Config config;
    if (args.length == 1) {
      config = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[0])))
              .withFallback(ConfigFactory.load("ds"));
    } else {
      config = ConfigFactory.load("ds");
    }

    final int port = config.getInt("port");
    final InetAddress host = InetAddress.getByName(config.getString("host"));
    final InetSocketAddress socketAddress = new InetSocketAddress(host, port);

    new WorkerApplication(config.getInt("id"), socketAddress, config.getString("zk_string")).run();
  }

  public void run() {
    final Config config = ConfigFactory.parseString("akka.remote.artery.canonical.port=" + this.host.getPort())
            .withFallback(ConfigFactory.parseString("akka.remote.artery.canonical.hostname=" + this.host.getHostString()))
            .withFallback(ConfigFactory.load("remote"));
    this.system = ActorSystem.create("worker", config);

    this.system.actorOf(LifecycleWatcher.props(this.zkConnectString, this.id), "watcher");
  }

  public void shutdown() {
    try {
      Await.ready(this.system.terminate(), Duration.Inf());
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
