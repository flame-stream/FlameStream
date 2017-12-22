package com.spbsu.flamestream.runtime.application;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class WorkerApplication {
  private final Logger log = LoggerFactory.getLogger(WorkerApplication.class);
  private final DumbInetSocketAddress host;
  private final String zkString;
  private final String id;

  @Nullable
  private ActorSystem system = null;

  public WorkerApplication(String id, DumbInetSocketAddress host, String zkString) {
    this.id = id;
    this.host = host;
    this.zkString = zkString;
  }

  public static void main(String... args) {
    if (args.length != 3) {
      throw new IllegalArgumentException("Usage: worker.jar <id> <host:port> <zkString>");
    }
    final String id = args[0];
    final DumbInetSocketAddress socketAddress = new DumbInetSocketAddress(args[1]);
    final String zkString = args[2];
    new WorkerApplication(id, socketAddress, zkString).run();
  }

  public void run() {
    log.info("Starting worker with id: '{}', host: '{}', zkString: '{}'", id, host, zkString);

    final Map<String, String> props = new HashMap<>();
    props.put("akka.remote.netty.tcp.hostname", host.host());
    props.put("akka.remote.netty.tcp.port", String.valueOf(host.port()));
    final Config config = ConfigFactory.parseMap(props).withFallback(ConfigFactory.load("remote"));

    this.system = ActorSystem.create("worker", config);
    system.actorOf(LifecycleWatcher.props(id, zkString), "watcher");
  }

  public void close() {
    try {
      if (system != null) {
        Await.ready(system.terminate(), Duration.Inf());
      }
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }
}
