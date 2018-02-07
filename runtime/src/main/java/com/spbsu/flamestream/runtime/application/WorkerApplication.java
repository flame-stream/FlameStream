package com.spbsu.flamestream.runtime.application;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

  public static void main(String... args) throws IOException {
    final Path cofigPath = Paths.get(args[0]);
    final ObjectMapper mapper = new ObjectMapper();
    final WorkerConfig workerConfig = mapper.readValue(
            Files.readAllBytes(cofigPath),
            WorkerConfig.class
    );
    final String id = workerConfig.id;
    final DumbInetSocketAddress socketAddress = workerConfig.localAddress;
    final String zkString = workerConfig.zkString;
    new WorkerApplication(id, socketAddress, zkString).run();
  }

  public void run() {
    log.info("Starting worker with id: '{}', host: '{}', zkString: '{}'", id, host, zkString);

    final Map<String, String> props = new HashMap<>();
    props.put("akka.remote.artery.canonical.hostname", host.host());
    props.put("akka.remote.artery.canonical.port", String.valueOf(host.port()));
    final Config config = ConfigFactory.parseMap(props).withFallback(ConfigFactory.load("remote"));

    this.system = ActorSystem.create("worker", config);
    system.actorOf(LifecycleWatcher.props(id, zkString), "watcher");

    system.registerOnTermination(() -> {
      try {
        Tracing.TRACING.flush(Paths.get("/tmp/trace.csv"));
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
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

  private static class WorkerConfig {
    private final String id;
    private final DumbInetSocketAddress localAddress;
    private final String zkString;

    private WorkerConfig(@JsonProperty("id") String id,
                         @JsonProperty("localAddress") String localAddress,
                         @JsonProperty("zkString") String zkString) {
      this.id = id;
      this.localAddress = new DumbInetSocketAddress(localAddress);
      this.zkString = zkString;
    }
  }
}
