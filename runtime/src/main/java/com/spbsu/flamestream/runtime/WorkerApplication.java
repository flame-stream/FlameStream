package com.spbsu.flamestream.runtime;

import akka.actor.ActorSystem;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spbsu.flamestream.runtime.config.AckerConfig;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import com.spbsu.flamestream.runtime.utils.tracing.Tracing;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class WorkerApplication implements Runnable {
  private final Logger log = LoggerFactory.getLogger(WorkerApplication.class);
  private final DumbInetSocketAddress host;
  private final String zkString;
  private final String id;
  private final String snapshotPath;

  @Nullable
  private ActorSystem system = null;

  static {
    try {
      Class.forName("org.agrona.concurrent.SleepingIdleStrategy");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public WorkerApplication(String id, DumbInetSocketAddress host, String zkString) {
    this(id, host, zkString, null);
  }

  public WorkerApplication(String id, DumbInetSocketAddress host, String zkString, String snapshotPath) {
    this.id = id;
    this.host = host;
    this.zkString = zkString;
    this.snapshotPath = snapshotPath;
  }

  public static void main(String... args) {
    final WorkerConfig workerConfig = new WorkerConfig();
    final String id = workerConfig.id();
    final DumbInetSocketAddress socketAddress = workerConfig.localAddress();
    final String zkString = workerConfig.zkString();
    if (workerConfig.guarantees() == Guarantees.AT_MOST_ONCE) {
      new WorkerApplication(id, socketAddress, zkString).run();
    } else {
      new WorkerApplication(id, socketAddress, zkString, workerConfig.snapshotPath()).run();
    }
  }

  @Override
  public void run() {
    log.info("Starting worker with id: '{}', host: '{}', zkString: '{}'", id, host, zkString);

    final Map<String, String> props = new HashMap<>();
    props.put("akka.remote.artery.canonical.hostname", host.host());
    props.put("akka.remote.artery.canonical.port", String.valueOf(host.port()));
    props.put("akka.remote.artery.bind.hostname", "0.0.0.0");
    props.put("akka.remote.artery.bind.port", String.valueOf(host.port()));
    try {
      final File shm = new File(("/dev/shm"));
      if (shm.exists() && shm.isDirectory()) {
        final String aeronDir = "/dev/shm/aeron-" + id;
        FileUtils.deleteDirectory(new File(aeronDir));
        props.put("akka.remote.artery.advanced.aeron-dir", aeronDir);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    final Config config = ConfigFactory.parseMap(props).withFallback(ConfigFactory.load("remote"));
    this.system = ActorSystem.create("worker", config);
    final AckerConfig ackerConfig = new AckerConfig(
            Integer.parseInt(System.getenv("MAX_ELEMENTS_IN_GRAPH")),
            Integer.parseInt(System.getenv("MILLIS_BETWEEN_COMMITS")),
            Integer.parseInt(System.getenv("DEFAULT_MINIMAL_TIME"))
    );
    //noinspection ConstantConditions
    system.actorOf(StartupWatcher.props(id, zkString, snapshotPath, ackerConfig), "watcher");

    system.registerOnTermination(() -> {
      try {
        Tracing.TRACING.flush(Paths.get("/tmp/trace.csv"));
      } catch (IOException e) {
        log.error("Something went wrong during trace flush", e);
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
    private final String snapshotPath;
    private final Guarantees guarantees;

    private WorkerConfig() {
      this(
              System.getenv("ID"),
              System.getenv("LOCAL_ADDRESS"),
              System.getenv("ZK_STRING"),
              System.getenv("SNAPSHOT_PATH"),
              Guarantees.valueOf(System.getenv("GUARANTEES"))
      );
    }

    private WorkerConfig(@JsonProperty("id") String id,
                         @JsonProperty("localAddress") String localAddress,
                         @JsonProperty("zkString") String zkString,
                         @JsonProperty("snapshotPath") String snapshotPath,
                         @JsonProperty("guarantees") Guarantees guarantees) {
      this.guarantees = guarantees;
      this.id = id;
      this.localAddress = new DumbInetSocketAddress(localAddress);
      this.zkString = zkString;
      this.snapshotPath = snapshotPath;
    }

    Guarantees guarantees() {
      return guarantees;
    }

    String snapshotPath() {
      return snapshotPath;
    }

    String id() {
      return id;
    }

    DumbInetSocketAddress localAddress() {
      return localAddress;
    }

    String zkString() {
      return zkString;
    }
  }

  @SuppressWarnings("unused")
  public enum Guarantees {
    AT_MOST_ONCE,
    AT_LEAST_ONCE,
    EXACTLY_ONCE
  }
}
