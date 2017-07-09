package com.spbsu.datastream.core.application;

import akka.actor.ActorSystem;
import com.spbsu.datastream.core.node.LifecycleWatcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jooq.lambda.Unchecked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Optional;
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

  public static void main(String... args) throws UnknownHostException {
    final int id = Integer.valueOf(System.getenv("DATASTREAMS_ID"));

    final InetAddress address = Optional.ofNullable(System.getenv("DATASTREAMS_HOST"))
            .map(Unchecked.function(InetAddress::getByName)).orElse(InetAddress.getLocalHost());
    final int port = Optional.ofNullable(System.getenv("DATASTREAMS_PORT"))
            .map(Integer::valueOf).orElse(4387);
    final String connectingString = System.getenv("DATASTREAMS_ZK");

    final InetSocketAddress socketAddress = new InetSocketAddress(address, port);

    new WorkerApplication(id, socketAddress, connectingString).run();
  }

  public void run() {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + this.host.getPort())
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + this.host.getHostString()))
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
