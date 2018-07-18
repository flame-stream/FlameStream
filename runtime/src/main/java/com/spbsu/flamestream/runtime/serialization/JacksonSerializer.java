package com.spbsu.flamestream.runtime.serialization;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class JacksonSerializer implements FlameSerializer {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    final SimpleModule module = new SimpleModule();
    module.addSerializer(ActorPath.class, new ActorPathSerializer(ActorPath.class));
    module.addDeserializer(ActorPath.class, new ActorPathDes(ActorPath.class));
    MAPPER.registerModule(module);
  }

  @Override
  public byte[] serialize(Object o) {
    try {
      return MAPPER.writeValueAsBytes(o);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> T deserialize(byte[] data, Class<T> clazz) {
    try {
      return MAPPER.readValue(data, clazz);
    } catch (IOException e) {
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
