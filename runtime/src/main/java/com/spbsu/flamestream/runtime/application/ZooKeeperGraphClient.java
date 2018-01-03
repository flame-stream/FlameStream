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
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ConfigurationClient;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ZooKeeperGraphClient implements AutoCloseable, ConfigurationClient {
  private static final int MAX_BUFFER_SIZE = 20000;
  private static final int BUFFER_SIZE = 1000;

  private final ZooKeeper zooKeeper;
  private final Kryo kryo;
  private final ObjectMapper mapper = new ObjectMapper();

  private final NavigableSet<String> seenGraphs = new TreeSet<>();

  public ZooKeeperGraphClient(ZooKeeper zooKeeper) {
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

  /**
   * Fetch graph, if present, and set watcher that would be called on updates
   */
  public List<ZooKeeperFlameClient> watchGraphs(Consumer<List<ZooKeeperFlameClient>> watcher) {
    try {
      createIfNotExists("/graphs");
      final List<String> children = zooKeeper.getChildren(
              "/graphs",
              event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                  watcher.accept(watchGraphs(watcher));
                }
              }
      );
      return children.stream()
              .map(c -> new ZooKeeperFlameClient("/graphs/" + c))
              .collect(Collectors.toList());
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public ZooKeeperFlameClient push(Graph graph) {
    try {
      createIfNotExists("/graphs");
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, graph);
      final String s = zooKeeper.create(
              "/graphs/graph",
              o.toBytes(),
              ZKUtil.parseACLs("world:anyone:crd"),
              CreateMode.PERSISTENT_SEQUENTIAL
      );
      return new ZooKeeperFlameClient(s);
    } catch (InterruptedException | KeeperException e) {
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
      final Stat exists = zooKeeper.exists("/config", false);
      if (exists != null) {
        zooKeeper.delete("/config", exists.getVersion());
      }
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

  public class ZooKeeperFlameClient implements AttachRegistry {
    private final String graphPath;
    private final Set<String> seenFronts = Collections.synchronizedSet(new HashSet<>());
    private final Set<String> seenRears = Collections.synchronizedSet(new HashSet<>());

    public ZooKeeperFlameClient(String graphPath) {
      this.graphPath = graphPath;
    }

    public String name() {
      return graphPath.split("/")[graphPath.split("/").length - 1];
    }

    public Graph graph() {
      try {
        final byte[] data = zooKeeper.getData(
                graphPath,
                false,
                null
        );
        final ByteBufferInput input = new ByteBufferInput(data);
        return (Graph) kryo.readClassAndObject(input);
      } catch (InterruptedException | KeeperException e) {
        throw new RuntimeException(e);
      }
    }

    public void removeGraph() {
      try {
        org.apache.zookeeper.ZKUtil.deleteRecursive(zooKeeper, graphPath);
      } catch (KeeperException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public void attachFront(String name, FlameRuntime.FrontInstance<?> instance) {
      try {
        createIfNotExists(graphPath + "/fronts");
        final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
        kryo.writeClassAndObject(o, instance);

        zooKeeper.create(
                graphPath + "/fronts/" + name,
                o.toBytes(),
                ZKUtil.parseACLs("world:anyone:crd"),
                CreateMode.PERSISTENT
        );
      } catch (InterruptedException | KeeperException e) {
        throw new RuntimeException(e);
      }
    }

    public void attachRear(String name, FlameRuntime.RearInstance<?> instance) {
      try {
        createIfNotExists(graphPath + "/rears");
        final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
        kryo.writeClassAndObject(o, instance);

        zooKeeper.create(
                graphPath + "/rears/" + name,
                o.toBytes(),
                ZKUtil.parseACLs("world:anyone:crd"),
                CreateMode.PERSISTENT
        );
      } catch (InterruptedException | KeeperException e) {
        throw new RuntimeException(e);
      }
    }

    public Set<AttachFront<?>> fronts(Consumer<Set<AttachFront<?>>> watcher) {
      try {
        final Stat exists = zooKeeper.exists(graphPath + "/fronts", event -> {
          if (event.getType() == Watcher.Event.EventType.NodeCreated) {
            final Set<AttachFront<?>> fronts = fronts(watcher);
            watcher.accept(fronts);
          }
        });
        if (exists != null) {
          return zooKeeper.getChildren(
                  graphPath + "/fronts",
                  event -> {
                    if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                      watcher.accept(fronts(watcher));
                    }
                  },
                  null
          )
                  .stream()
                  .filter(name -> !seenFronts.contains(name))
                  .peek(seenFronts::add)
                  .map(name -> new AttachFront<>(name, frontBy(name)))
                  .collect(Collectors.toSet());
        } else {
          return Collections.emptySet();
        }
      } catch (KeeperException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    private FlameRuntime.FrontInstance<?> frontBy(String name) {
      try {
        final byte[] data = zooKeeper.getData(graphPath + "/fronts/" + name, false, null);
        final ByteBufferInput input = new ByteBufferInput(data);
        return (FlameRuntime.FrontInstance<?>) kryo.readClassAndObject(input);
      } catch (KeeperException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public Set<AttachRear<?>> rears(Consumer<Set<AttachRear<?>>> watcher) {
      try {
        final Stat exists = zooKeeper.exists(graphPath + "/rears", event -> {
          if (event.getType() == Watcher.Event.EventType.NodeCreated) {
            final Set<AttachRear<?>> rears = rears(watcher);
            watcher.accept(rears);
          }
        });
        if (exists != null) {
          final List<String> children = zooKeeper.getChildren(
                  graphPath + "/rears",
                  event -> {
                    if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                      watcher.accept(rears(watcher));
                    }
                  },
                  null
          );

          return children.stream()
                  .filter(name -> !seenRears.contains(name))
                  .peek(seenFronts::add)
                  .map(name -> new AttachRear<>(name, rearBy(name)))
                  .collect(Collectors.toSet());
        } else {
          return Collections.emptySet();
        }
      } catch (KeeperException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    private FlameRuntime.RearInstance<?> rearBy(String name) {
      try {
        // There is no watcher because fronts are immutable
        final byte[] data = zooKeeper.getData(graphPath + "/rears/" + name, false, null);
        final ByteBufferInput input = new ByteBufferInput(data);
        return (FlameRuntime.RearInstance<?>) kryo.readClassAndObject(input);
      } catch (KeeperException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void register(EdgeId frontId, long attachTimestamp) {
      try {
        zooKeeper.create(
                graphPath + "/fronts/" + frontId.edgeName() + '/' + frontId.nodeId(),
                new byte[0],
                ZKUtil.parseACLs("world:anyone:crd"),
                CreateMode.PERSISTENT
        );

        final byte[] attachTs = new byte[8];
        ByteBuffer.wrap(attachTs).putLong(attachTimestamp);
        zooKeeper.create(
                graphPath + "/fronts/" + frontId.edgeName() + '/' + frontId.nodeId() + "/attachTs",
                attachTs,
                ZKUtil.parseACLs("world:anyone:crd"),
                CreateMode.PERSISTENT
        );
      } catch (KeeperException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void createIfNotExists(String path) throws KeeperException, InterruptedException {
    try {
      zooKeeper.create(
              path,
              new byte[0],
              ZKUtil.parseACLs("world:anyone:crd"),
              CreateMode.PERSISTENT
      );
    } catch (KeeperException k) {
      if (k.code() != KeeperException.Code.NODEEXISTS) {
        throw k;
      }
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