package com.spbsu.flamestream.runtime.environment.remote;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.RootActorPath;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spbsu.flamestream.core.graph.AtomicGraph;
import com.spbsu.flamestream.runtime.DumbInetSocketAddress;
import com.spbsu.flamestream.runtime.configuration.KryoInfoSerializer;
import com.spbsu.flamestream.runtime.configuration.TickInfoSerializer;
import com.spbsu.flamestream.runtime.environment.CollectingActor;
import com.spbsu.flamestream.runtime.environment.Environment;
import com.spbsu.flamestream.runtime.raw.SingleRawData;
import com.spbsu.flamestream.runtime.tick.TickInfo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

public final class RemoteEnvironment implements Environment {
  private static final String SYSTEM_NAME = "remote-environment";
  private static final int SYSTEM_PORT = 12345;
  private final Logger log = LoggerFactory.getLogger(RemoteEnvironment.class);
  private final InetAddress environmentAddress;

  private final ObjectMapper mapper = new ObjectMapper();

  private final TickInfoSerializer serializer = new KryoInfoSerializer();
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
        serializer.serialize(tickInfo),
        ZKUtil.parseACLs("world:anyone:crdwa"),
        CreateMode.PERSISTENT
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<Integer> availableFronts() {
    try {
      final byte[] data = zooKeeper.getData("/fronts", false, new Stat());
      return mapper.readValue(data, new TypeReference<Set<Integer>>() {});
    } catch (IOException | KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Set<Integer> availableWorkers() {
    return dns().keySet();
  }

  @Override
  public <T> AtomicGraph wrapInSink(ToIntFunction<? super T> hash, Consumer<? super T> mySuperConsumer) {
    final String suffix = UUID.randomUUID().toString();
    localSystem.actorOf(CollectingActor.props(mySuperConsumer), suffix);
    return new RemoteActorSink<>(hash, wrapperPath(suffix));
  }

  private ActorPath wrapperPath(String suffix) {
    final Address address = new Address("akka.tcp", SYSTEM_NAME, environmentAddress.getHostName(), SYSTEM_PORT);

    return RootActorPath.apply(address, "/").child("user").child(suffix);
  }

  @Override
  public Consumer<Object> frontConsumer(int frontId) {
    final DumbInetSocketAddress front = dns().get(frontId);
    if (front == null) {
      throw new IllegalArgumentException("There is no front with id " + frontId);
    }

    final ActorSelection frontSelection = localSystem.actorSelection(frontPath(front));

    return object -> frontSelection.tell(new SingleRawData<>(object), ActorRef.noSender());
  }

  private ActorPath frontPath(DumbInetSocketAddress frontAddress) {
    final Address address = new Address("akka.tcp", "worker", frontAddress.host(), frontAddress.port());

    return RootActorPath.apply(address, "/").child("user").child("watcher").child("concierge").child("front");
  }

  private Map<Integer, DumbInetSocketAddress> dns() {
    try {
      final byte[] data = zooKeeper.getData("/dns", false, new Stat());
      return mapper.readValue(data, new TypeReference<Map<Integer, DumbInetSocketAddress>>() {});
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
