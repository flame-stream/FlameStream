package com.spbsu.flamestream.runtime.environment.remote;

import akka.actor.ActorPath;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.RootActorPath;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.core.graph.HashFunction;
import com.spbsu.flamestream.runtime.utils.DumbInetSocketAddress;
import com.spbsu.flamestream.runtime.utils.serialization.CommonSerializer;
import com.spbsu.flamestream.runtime.utils.serialization.FrontSerializer;
import com.spbsu.flamestream.runtime.utils.serialization.TickInfoSerializer;
import com.spbsu.flamestream.runtime.environment.CollectingActor;
import com.spbsu.flamestream.runtime.environment.Environment;
import com.spbsu.flamestream.runtime.node.tick.api.TickInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public final class RemoteEnvironment implements Environment {
  private static final String SYSTEM_NAME = "remote-environment";
  private static final int SYSTEM_PORT = 12345;
  private final Logger log = LoggerFactory.getLogger(RemoteEnvironment.class);
  private final InetAddress environmentAddress;

  private final ObjectMapper mapper = new ObjectMapper();

  private final TickInfoSerializer tickInfoSerializer = new CommonSerializer();
  private final FrontSerializer frontSerializer = new CommonSerializer();
  private final ZooKeeper zooKeeper;
  private final ActorSystem localSystem;

  public RemoteEnvironment(String zookeeperString) {
    try {
      this.environmentAddress = InetAddress.getLocalHost();
      final Config config = ConfigFactory.parseString("akka.remote.netty.tcp.port=" + SYSTEM_PORT)
              .withFallback(ConfigFactory.parseString(
                      "akka.remote.netty.tcp.hostname=" + environmentAddress.getHostName()))
              .withFallback(ConfigFactory.load("remote"));

      localSystem = ActorSystem.create(SYSTEM_NAME, config);

      this.zooKeeper = new ZooKeeper(zookeeperString, 5000, e -> log.info("Init zookeeperString ZKEvent: {}", e));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void deploy(TickInfo tickInfo) {
    try {
      zooKeeper.create(
              "/ticks/" + tickInfo.id(),
              tickInfoSerializer.serialize(tickInfo),
              ZKUtil.parseACLs("world:anyone:crdwa"),
              CreateMode.PERSISTENT
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deployFront(String nodeId, String frontId, Props frontProps) {
    try {
      zooKeeper.create(
              "/workers/" + nodeId + "/fronts/" + frontId,
              frontSerializer.serialize(frontProps),
              ZKUtil.parseACLs("world:anyone:crdwa"),
              CreateMode.PERSISTENT
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<String> availableWorkers() {
    return dns().keySet();
  }

  @Override
  public <T> AtomicGraph wrapInSink(HashFunction<? super T> hash, Consumer<? super T> mySuperConsumer) {
    final String suffix = UUID.randomUUID().toString();
    localSystem.actorOf(CollectingActor.props(mySuperConsumer), suffix);
    return new RemoteActorSink<>(hash, wrapperPath(suffix));
  }

  @Override
  public void awaitTick(long tickId) throws InterruptedException {
    final Object monitor = new Object();
    final AtomicBoolean exists = new AtomicBoolean(false);

    synchronized (monitor) {
      try {
        final Stat stat = zooKeeper.exists("/ticks/" + tickId + "/committed", w -> {
          if (w.getType() == Watcher.Event.EventType.NodeCreated) {
            synchronized (monitor) {
              exists.set(true);
              monitor.notifyAll();
            }
          }
        });

        while (stat == null && !exists.get()) {
          monitor.wait();
        }
      } catch (KeeperException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Set<Long> ticks() {
    try {
      final List<String> children = zooKeeper.getChildren("/ticks", false);
      return children.stream().map(Long::parseLong).collect(Collectors.toSet());
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private ActorPath wrapperPath(String suffix) {
    final Address address = new Address("akka.tcp", SYSTEM_NAME, environmentAddress.getHostName(), SYSTEM_PORT);
    return RootActorPath.apply(address, "/").child("user").child(suffix);
  }

  private Map<String, DumbInetSocketAddress> dns() {
    try {
      final byte[] data = zooKeeper.getData("/dns", false, new Stat());
      return mapper.readValue(data, new TypeReference<Map<String, DumbInetSocketAddress>>() {
      });
    } catch (IOException | InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      zooKeeper.close();
      Await.ready(localSystem.terminate(), Duration.Inf());
    } catch (InterruptedException e) {
      log.error("Smth bad happens during closing ZookeeperClient", e);
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      log.error("Can't terminate environment system", e);
    }
  }
}
