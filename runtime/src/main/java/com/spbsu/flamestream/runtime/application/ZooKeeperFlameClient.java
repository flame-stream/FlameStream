package com.spbsu.flamestream.runtime.application;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.spbsu.flamestream.core.Graph;
import com.spbsu.flamestream.core.data.meta.EdgeId;
import com.spbsu.flamestream.runtime.FlameRuntime;
import com.spbsu.flamestream.runtime.acker.AttachRegistry;
import com.spbsu.flamestream.runtime.client.AdminClient;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ZooKeeperFlameClient implements AttachRegistry, AutoCloseable, AdminClient {
  private static final int MAX_BUFFER_SIZE = 20000;
  private static final int BUFFER_SIZE = 1000;

  private final ZooKeeper zooKeeper;
  private final Kryo kryo;
  private final ObjectMapper mapper = new ObjectMapper();

  private final Set<String> seenFronts = Collections.synchronizedSet(new HashSet<>());
  private final Set<String> seenRears = Collections.synchronizedSet(new HashSet<>());

  public ZooKeeperFlameClient(ZooKeeper zooKeeper) {
    this.zooKeeper = zooKeeper;

    final SimpleModule module = new SimpleModule();
    module.addSerializer(ActorPath.class, new ActorPathSerializer(ActorPath.class));
    module.addDeserializer(ActorPath.class, new ActorPathDes(ActorPath.class));
    mapper.registerModule(module);

    this.kryo = new Kryo();
    ((Kryo.DefaultInstantiatorStrategy) kryo.getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
  }

  @Override
  public void close() throws Exception {
    zooKeeper.close();
  }

  public void attachFront(String name, FlameRuntime.FrontInstance<?> instance) {
    try {
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, instance);

      zooKeeper.create(
              "/graph/fronts/" + name,
              o.toBytes(),
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public void attachRear(String name, FlameRuntime.RearInstance<?> instance) {
    try {
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, instance);

      zooKeeper.create(
              "/graph/rears/" + name,
              o.toBytes(),
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public Set<AttachFront<?>> fronts(Consumer<Set<AttachFront<?>>> watcher) {
    try {
      return zooKeeper.getChildren(
              "/graph/fronts",
              event -> watcher.accept(fronts(watcher)),
              null
      )
              .stream()
              .filter(name -> !seenFronts.contains(name))
              .peek(seenFronts::add)
              .map(name -> new AttachFront<>(name, frontBy(name)))
              .collect(Collectors.toSet());
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private FlameRuntime.FrontInstance<?> frontBy(String name) {
    try {
      final byte[] data = zooKeeper.getData("/graph/fronts/" + name, false, null);
      final ByteBufferInput input = new ByteBufferInput(data);
      return (FlameRuntime.FrontInstance<?>) kryo.readClassAndObject(input);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Set<AttachRear<?>> rears(Consumer<Set<AttachRear<?>>> watcher) {
    try {
      return zooKeeper.getChildren(
              "/graph/rears",
              event -> watcher.accept(rears(watcher)),
              null
      )
              .stream()
              .filter(name -> !seenRears.contains(name))
              .peek(seenFronts::add)
              .map(name -> new AttachRear<>(name, rearBy(name)))
              .collect(Collectors.toSet());
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private FlameRuntime.RearInstance<?> rearBy(String name) {
    try {
      // There is no watcher because fronts are immutable
      final byte[] data = zooKeeper.getData("/graph/rears/" + name, false, null);
      final ByteBufferInput input = new ByteBufferInput(data);
      return (FlameRuntime.RearInstance<?>) kryo.readClassAndObject(input);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetch graph, if present, and set watcher that would be called on updates
   */
  public Optional<Graph> graph(Consumer<Graph> watcher) {
    try {
      final Stat exists = zooKeeper.exists(
              "/graph",
              e -> watcher.accept(graph())
      );

      if (exists != null) {
        return Optional.of(graph());
      } else {
        return Optional.empty();
      }
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }


  private Graph graph() {
    try {
      final byte[] data = zooKeeper.getData(
              "/graph",
              false,
              null
      );
      final ByteBufferInput input = new ByteBufferInput(data);
      return (Graph) kryo.readClassAndObject(input);
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public void push(Graph graph) {
    try {
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, graph);

      // ACL is so specific (cr) to forbid graph updates. There is no support yet
      zooKeeper.create(
              "/graph",
              o.toBytes(),
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
      zooKeeper.create(
              "/graph/fronts",
              new byte[0],
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
      zooKeeper.create(
              "/graph/rears",
              new byte[0],
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public void removeCurrentGraph() {
    try {
      final Stat exists = zooKeeper.exists(
              "/graph",
              e -> {}
      );
      if (exists != null) {
        zooKeeper.delete("/graph", exists.getVersion());
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ClusterConfig config() {
    try {
      final byte[] data = zooKeeper.getData("/config", false, null);
      return mapper.readValue(data, ClusterConfig.class);
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(ClusterConfig config) {
    try {
      zooKeeper.create(
              "/config",
              mapper.writeValueAsBytes(config),
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
    } catch (JsonProcessingException | InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void register(EdgeId frontId, long attachTimestamp) {
    try {
      zooKeeper.create(
              "/graph/fronts/" + frontId.edgeName() + '/' + frontId.nodeId(),
              new byte[0],
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );

      final byte[] attachTs = new byte[8];
      ByteBuffer.wrap(attachTs).putLong(attachTimestamp);
      zooKeeper.create(
              "/graph/fronts/" + frontId.edgeName() + '/' + frontId.nodeId() + "/attachTs",
              attachTs,
              ZKUtil.parseACLs("world:anyone:cr"),
              CreateMode.PERSISTENT
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("serial")
  private static class ActorPathDes extends StdDeserializer<ActorPath> {
    protected ActorPathDes(Class<?> vc) {
      super(vc);
    }

    @Override
    public ActorPath deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return ActorPaths.fromString(p.getText());
    }
  }

  @SuppressWarnings("serial")
  private static class ActorPathSerializer extends StdSerializer<ActorPath> {
    protected ActorPathSerializer(Class<ActorPath> t) {
      super(t);
    }

    @Override
    public void serialize(ActorPath value, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeString(value.toSerializationFormat());
    }
  }
}