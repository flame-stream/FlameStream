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
import com.spbsu.flamestream.runtime.acker.Registry;
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ConfigurationClient;
import com.spbsu.flamestream.runtime.edge.api.AttachFront;
import com.spbsu.flamestream.runtime.edge.api.AttachRear;
import org.apache.hadoop.util.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

public class ZooKeeperGraphClient implements AutoCloseable, ConfigurationClient, Registry {
  private static final int MAX_BUFFER_SIZE = 20000;
  private static final int BUFFER_SIZE = 1000;
  private static final List<ACL> DEFAULT_ACL = ZKUtil.parseACLs("world:anyone:crwd");

  private final ZooKeeper zooKeeper;
  private final Kryo kryo;
  private final ObjectMapper mapper = new ObjectMapper();

  private final Set<String> seenFronts = Collections.synchronizedSet(new HashSet<>());
  private final Set<String> seenRears = Collections.synchronizedSet(new HashSet<>());

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

  public void push(Graph graph) {
    try {
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, graph);
      zooKeeper.create(
              "/graph",
              o.toBytes(),
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Watcher is called when the graph appears
   */
  public void watchGraph(Consumer<Boolean> watcher) {
    try {
      final Stat exists = zooKeeper.exists("/graph", event -> {
        if (event.getType() == Watcher.Event.EventType.NodeCreated) {
          watcher.accept(true);
        }
      });
      if (exists != null) {
        watcher.accept(true);
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Graph graph() {
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

  public void attachFront(String name, FlameRuntime.FrontInstance<?> instance) {
    try {
      createIfNotExists("/graph/fronts");
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, instance);

      zooKeeper.create(
              "/graph/fronts/" + name,
              o.toBytes(),
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public void attachRear(String name, FlameRuntime.RearInstance<?> instance) {
    try {
      createIfNotExists("/graph/rears");
      final ByteBufferOutput o = new ByteBufferOutput(BUFFER_SIZE, MAX_BUFFER_SIZE);
      kryo.writeClassAndObject(o, instance);

      zooKeeper.create(
              "/graph/rears/" + name,
              o.toBytes(),
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );
    } catch (InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  public Set<AttachFront<?>> fronts(Consumer<Set<AttachFront<?>>> watcher) {
    try {
      final Stat exists = zooKeeper.exists("/graph/fronts", event -> {
        if (event.getType() == Watcher.Event.EventType.NodeCreated) {
          final Set<AttachFront<?>> fronts = fronts(watcher);
          watcher.accept(fronts);
        }
      });
      if (exists != null) {
        return zooKeeper.getChildren(
                "/graph/fronts",
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

  @Override
  public int epoch(IntConsumer epochWatcher) {
    try {
      final Stat exists = zooKeeper.exists("/epoch", event -> {
        if (event.getType() == Watcher.Event.EventType.NodeCreated) {
          epochWatcher.accept(epoch(epochWatcher));
        }
      });

      if (exists == null) {
        return -1;
      }

      final byte[] data = zooKeeper.getData("/epoch", event -> {
        if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
          epochWatcher.accept(epoch(epochWatcher));
        }
      }, null);
      return Integer.parseInt(new String(data));
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void setEpoch(int epoch) {
    try {
      final Stat exists = zooKeeper.exists("/epoch", false);
      final byte[] bytes = Integer.toString(epoch).getBytes();
      if (exists != null) {
        zooKeeper.setData("/epoch", bytes, exists.getVersion());
      } else {
        zooKeeper.create("/epoch", bytes, DEFAULT_ACL, CreateMode.PERSISTENT);
      }
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
      final Stat exists = zooKeeper.exists("/graph/rears", event -> {
        if (event.getType() == Watcher.Event.EventType.NodeCreated) {
          final Set<AttachRear<?>> rears = rears(watcher);
          watcher.accept(rears);
        }
      });
      if (exists != null) {
        final List<String> children = zooKeeper.getChildren("/graph/rears", event -> {
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
      final byte[] data = zooKeeper.getData("/graph/rears/" + name, false, null);
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
              "/graph/fronts/" + frontId.edgeName() + '/' + frontId.nodeId(),
              new byte[0],
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );

      zooKeeper.create(
              "/graph/fronts/" + frontId.edgeName() + '/' + frontId.nodeId() + "/attachTs",
              Long.toString(attachTimestamp).getBytes(),
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long registeredTime(EdgeId frontId) {
    try {
      final String frontPath = "/graph/fronts/" + frontId.edgeName() + '/' + frontId.nodeId() + "/attachTs";
      final Stat exists = zooKeeper.exists(frontPath, false);
      if (exists != null) {
        return Long.parseLong(new String(zooKeeper.getData(frontPath, false, null)));
      } else {
        return -1;
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void committed(long time) {
    try {
      final String lastCommitPath = "/graph/last-commit";
      createIfNotExists(lastCommitPath);
      zooKeeper.setData(lastCommitPath, Long.toString(time).getBytes(), -1);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long lastCommit() {
    try {
      final String lastCommitPath = "/graph/last-commit";
      final Stat exists = zooKeeper.exists(lastCommitPath, false);
      if (exists != null) {
        return Long.parseLong(new String(zooKeeper.getData(lastCommitPath, false, null)));
      } else {
        return 0;
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
      final Stat exists = zooKeeper.exists("/config", false);
      if (exists != null) {
        zooKeeper.delete("/config", exists.getVersion());
      }
      zooKeeper.create(
              "/config",
              mapper.writeValueAsBytes(config),
              DEFAULT_ACL,
              CreateMode.PERSISTENT
      );
    } catch (JsonProcessingException | InterruptedException | KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  private void createIfNotExists(String path) throws KeeperException, InterruptedException {
    try {
      zooKeeper.create(
              path,
              new byte[0],
              DEFAULT_ACL,
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