package com.spbsu.flamestream.runtime.application;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

public class WorkerApplication {
  private final Logger log = LoggerFactory.getLogger(WorkerApplication.class);
  private final DumbInetSocketAddress host;
  private final String zkString;
  private final String id;

  private ActorSystem system = null;

  public WorkerApplication(String id, DumbInetSocketAddress host, String zkString) {
    this.id = id;
    this.host = host;
    this.zkString = zkString;
  }

  public static void main(String... args) throws IOException {
    final Config config = ConfigFactory.parseReader(Files.newBufferedReader(Paths.get(args[0])));

    final String id = config.getString("id");
    final DumbInetSocketAddress socketAddress = new DumbInetSocketAddress(
            config.getString("host"),
            config.getInt("port")
    );
    final String zkString = config.getString("zk_string");

    new WorkerApplication(id, socketAddress, zkString).run();
  }

  public void run() {
    log.info("Starting worker with id: '{}', host: '{}', zkString: '{}'", id, host, zkString);
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + host.port())
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + host.host()))
            .withFallback(ConfigFactory.load("remote"));
    this.system = ActorSystem.create("worker", config);
    system.actorOf(LifecycleWatcher.props(id, zkString), "watcher");
  }

  public void close() {
    try {
      Await.ready(system.terminate(), Duration.Inf());
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
