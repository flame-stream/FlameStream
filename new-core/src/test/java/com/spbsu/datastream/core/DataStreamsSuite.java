package com.spbsu.datastream.core;

import akka.actor.ActorPath;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import com.spbsu.datastream.core.application.WorkerApplication;
import com.spbsu.datastream.core.application.ZooKeeperApplication;
import com.spbsu.datastream.core.configuration.HashRange;
import com.spbsu.datastream.core.node.MyPaths;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.zookeeper.KeeperException;
import org.jooq.lambda.Unchecked;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class DataStreamsSuite {
  //Suite data
  private final Map<HashRange, InetSocketAddress> workers = DataStreamsSuite.workers(10);
  private final Map<Integer, InetSocketAddress> fronts = DataStreamsSuite.fronts(this.workers);

  //Test method data
  private final Collection<WorkerApplication> workerApplication = new HashSet<>();
  private ActorSystem localSystem;

  @BeforeSuite
  public final void beforeSuite() throws IOException, InterruptedException {
    final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + 12341)
            .withFallback(ConfigFactory.parseString("akka.remote.netty.tcp.hostname=" + InetAddress.getLoopbackAddress().getHostName()))
            .withFallback(ConfigFactory.load("remote"));
    this.localSystem = ActorSystem.create("requester", config);

    new Thread(Unchecked.runnable(() -> new ZooKeeperApplication().run())).start();
    TimeUnit.SECONDS.sleep(5);
  }

  @AfterSuite
  public final void afterSuite() throws IOException, InterruptedException {
    // TODO: 5/2/17 Graceful stop
    this.localSystem.shutdown();

    // TODO: 5/2/17 kill ZK
    Files.newDirectoryStream(Paths.get("zookeeper", "version-2")).forEach(Unchecked.consumer(Files::delete));
  }

  @BeforeMethod
  public final void prepareEnvironment() throws IOException, KeeperException, InterruptedException {
    try (final ZK zkConfigurationDeployer = new ZK("localhost:2181")) {
      zkConfigurationDeployer.pushFrontMappings(this.fronts);
      zkConfigurationDeployer.pushRangeMappings(this.workers);
    }

    for (final InetSocketAddress workerAddress : this.workers.values()) {
      final WorkerApplication worker = new WorkerApplication(workerAddress, "localhost:2181");
      final Thread workerThread = new Thread(worker::run);

      this.workerApplication.add(worker);
      workerThread.start();
    }
  }

  @AfterMethod
  public final void clearEnvironment() {
    this.workerApplication.forEach(WorkerApplication::shutdown);
    this.workerApplication.clear();
  }

  protected Map<Integer, ActorSelection> frontRawReceivers() throws InterruptedException, IOException, KeeperException {
    return this.fronts.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> DataStreamsSuite.front(this.localSystem, e.getValue())));
  }

  private static Map<Integer, InetSocketAddress> fronts(
          final Map<HashRange, InetSocketAddress> correspondingWorkers) {
    try {
      final Map<Integer, InetSocketAddress> fronts = new HashMap<>();

      int port = 5223;

      for (final HashRange range : correspondingWorkers.keySet()) {
        fronts.put(range.from(), new InetSocketAddress(InetAddress.getLocalHost(), port));
        port += 1;
      }

      return fronts;
    } catch (final UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<HashRange, InetSocketAddress> workers(final int count) {
    try {
      final int step = (int) (((long) Integer.MAX_VALUE - Integer.MIN_VALUE) / count);

      final Map<HashRange, InetSocketAddress> workers = new HashMap<>();

      for (int left = Integer.MIN_VALUE, right = left + step, port = 5123; right <= Integer.MAX_VALUE - step; left += step, right += step, port += 1) {
        workers.put(new HashRange(left, right), new InetSocketAddress(InetAddress.getLocalHost(), port));
      }

      return workers;
    } catch (final UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("TypeMayBeWeakened")
  private static ActorSelection nodeConcierge(final ActorSystem system,
                                              final int port) {
    final InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    final ActorPath rangeConcierge = MyPaths.nodeConcierge(address);
    return system.actorSelection(rangeConcierge);
  }

  @SuppressWarnings("TypeMayBeWeakened")
  private static ActorSelection front(final ActorSystem system,
                                      final InetSocketAddress address) {
    final ActorPath front = MyPaths.front(address);
    return system.actorSelection(front);
  }
}
