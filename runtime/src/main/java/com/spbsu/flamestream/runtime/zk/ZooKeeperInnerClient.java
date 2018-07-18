package com.spbsu.flamestream.runtime.zk;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
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
import com.spbsu.flamestream.runtime.config.ClusterConfig;
import com.spbsu.flamestream.runtime.config.ConfigurationClient;
import com.spbsu.flamestream.runtime.master.acker.Registry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZooKeeperInnerClient extends ZooKeeperBaseClient implements ConfigurationClient, Registry {
  private final ObjectMapper mapper = new ObjectMapper();

  public ZooKeeperInnerClient(ZooKeeper zooKeeper) {
    super(zooKeeper);

    final SimpleModule module = new SimpleModule();
    module.addSerializer(ActorPath.class, new ActorPathSerializer(ActorPath.class));
    module.addDeserializer(ActorPath.class, new ActorPathDes(ActorPath.class));
    mapper.registerModule(module);
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

  @SuppressWarnings("serial")
  private static class ActorPathDes extends StdDeserializer<ActorPath> {
    ActorPathDes(Class<?> vc) {
      super(vc);
    }

    @Override
    public ActorPath deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return ActorPaths.fromString(p.getText());
    }
  }

  @SuppressWarnings("serial")
  private static class ActorPathSerializer extends StdSerializer<ActorPath> {
    ActorPathSerializer(Class<ActorPath> t) {
      super(t);
    }

    @Override
    public void serialize(ActorPath value, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeString(value.toSerializationFormat());
    }
  }
}