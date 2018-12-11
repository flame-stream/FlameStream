package com.spbsu.flamestream.runtime;

import akka.actor.ActorSystem;
import com.spbsu.flamestream.runtime.config.CommitterConfig;
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
  private final WorkerConfig workerConfig;

  @Nullable
  private ActorSystem system = null;

  static {
    try {
      Class.forName("org.agrona.concurrent.SleepingIdleStrategy");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("WeakerAccess")
  public WorkerApplication(WorkerConfig workerConfig) {
    this.workerConfig = workerConfig;
  }

  public static void main(String... args) {
    final WorkerConfig config = new WorkerConfig.Builder()
            .snapshotPath(System.getenv("SNAPSHOT_PATH"))
            .guarantees(Guarantees.valueOf(System.getenv("GUARANTEES")))
            .defaultMinimalTime(Integer.parseInt(System.getenv("DEFAULT_MINIMAL_TIME")))
            .millisBetweenCommits(Integer.parseInt(System.getenv("MILLIS_BETWEEN_COMMITS")))
            .maxElementsInGraph(Integer.parseInt(System.getenv("MAX_ELEMENTS_IN_GRAPH")))
            .build(
                    System.getenv("ID"),
                    new DumbInetSocketAddress(System.getenv("LOCAL_ADDRESS")),
                    System.getenv("ZK_STRING")
            );
    new WorkerApplication(config).run();
  }

  @Override
  public void run() {
    log.info("Starting worker with workerConfig '{}'", workerConfig);

    final Map<String, String> props = new HashMap<>();
    props.put("akka.remote.artery.canonical.hostname", workerConfig.localAddress.host());
    props.put("akka.remote.artery.canonical.port", String.valueOf(workerConfig.localAddress.port()));
    props.put("akka.remote.artery.bind.hostname", "0.0.0.0");
    props.put("akka.remote.artery.bind.port", String.valueOf(workerConfig.localAddress.port()));
    try {
      final File shm = new File(("/dev/shm"));
      if (shm.exists() && shm.isDirectory()) {
        final String aeronDir = "/dev/shm/aeron-" + workerConfig.id;
        FileUtils.deleteDirectory(new File(aeronDir));
        props.put("akka.remote.artery.advanced.aeron-dir", aeronDir);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    final Config config = ConfigFactory.parseMap(props).withFallback(ConfigFactory.load("remote"));
    this.system = ActorSystem.create("worker", config);
    final CommitterConfig committerConfig = new CommitterConfig(
            workerConfig.maxElementsInGraph,
            workerConfig.millisBetweenCommits,
            workerConfig.defaultMinimalTime
    );
    //noinspection ConstantConditions
    system.actorOf(
            StartupWatcher.props(workerConfig.id, workerConfig.zkString, workerConfig.snapshotPath, committerConfig),
            "watcher"
    );

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

  public static class WorkerConfig {
    private final String id;
    private final DumbInetSocketAddress localAddress;
    private final String zkString;

    private final String snapshotPath;
    private final Guarantees guarantees;

    private final int maxElementsInGraph;
    private final int millisBetweenCommits;
    private final int defaultMinimalTime;

    private WorkerConfig(String id,
                         DumbInetSocketAddress localAddress,
                         String zkString,
                         String snapshotPath,
                         Guarantees guarantees,
                         int maxElementsInGraph, int millisBetweenCommits, int defaultMinimalTime) {
      this.guarantees = guarantees;
      this.id = id;
      this.localAddress = localAddress;
      this.zkString = zkString;
      this.snapshotPath = snapshotPath;
      this.maxElementsInGraph = maxElementsInGraph;
      this.millisBetweenCommits = millisBetweenCommits;
      this.defaultMinimalTime = defaultMinimalTime;
    }

    @Override
    public String toString() {
      return "WorkerConfig{" +
              "id='" + id + '\'' +
              ", localAddress=" + localAddress +
              ", zkString='" + zkString + '\'' +
              ", snapshotPath='" + snapshotPath + '\'' +
              ", guarantees=" + guarantees +
              ", maxElementsInGraph=" + maxElementsInGraph +
              ", millisBetweenCommits=" + millisBetweenCommits +
              ", defaultMinimalTime=" + defaultMinimalTime +
              '}';
    }

    static class Builder {
      private String snapshotPath = null;
      private Guarantees guarantees = Guarantees.AT_MOST_ONCE;

      private int maxElementsInGraph = 500;
      private int millisBetweenCommits = 100;
      private int defaultMinimalTime = 0;

      Builder snapshotPath(String snapshotPath) {
        this.snapshotPath = snapshotPath;
        return this;
      }

      Builder guarantees(Guarantees guarantees) {
        this.guarantees = guarantees;
        return this;
      }

      public Builder maxElementsInGraph(int maxElementsInGraph) {
        this.maxElementsInGraph = maxElementsInGraph;
        return this;
      }

      public Builder millisBetweenCommits(int millisBetweenCommits) {
        this.millisBetweenCommits = millisBetweenCommits;
        return this;
      }

      Builder defaultMinimalTime(int defaultMinimalTime) {
        this.defaultMinimalTime = defaultMinimalTime;
        return this;
      }

      WorkerConfig build(String id, DumbInetSocketAddress localAddress, String zkString) {
        return new WorkerConfig(
                id,
                localAddress,
                zkString,
                snapshotPath,
                guarantees,
                maxElementsInGraph,
                millisBetweenCommits,
                defaultMinimalTime
        );
      }
    }
  }

  @SuppressWarnings("unused")
  public enum Guarantees {
    AT_MOST_ONCE,
    AT_LEAST_ONCE,
    EXACTLY_ONCE
  }
}
