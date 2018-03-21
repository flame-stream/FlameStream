package com.spbsu.flamestream.runtime.state;

import akka.serialization.Serialization;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.graph.state.GroupingState;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.util.Try;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RedisStateStorage implements StateStorage {
  private final JedisPool jedisPool;
  private final Serialization serialization;

  public RedisStateStorage(String host, int port, Serialization serialization) {
    this.serialization = serialization;
    this.jedisPool = new JedisPool(host, port);
  }

  @Override
  public Map<String, GroupingState> stateFor(HashUnit unit, GlobalTime time) {
    try (final Jedis jedis = jedisPool.getResource()) {
      final String key = unit.id() + '@' + time.toString();
      final byte[] data = jedis.get(key.getBytes());
      if (data != null) {
        final Try<Map> trie = serialization.deserialize(data, Map.class);
        //noinspection unchecked
        return (Map<String, GroupingState>) trie.get();
      } else {
        return new ConcurrentHashMap<>();
      }
    }
  }

  @Override
  public void putState(HashUnit unit, GlobalTime time, Map<String, GroupingState> state) {
    try (final Jedis jedis = jedisPool.getResource()) {
      final Set<String> keys = jedis.keys(unit.id() + '*');
      if (keys.size() == 2) {
        final String min = keys.stream().min(String::compareTo).get();
        jedis.del(min);
      }

      final String key = unit.id() + '@' + time.toString();
      final byte[] data = serialization.serialize(state).get();
      jedis.set(key.getBytes(), data);
    }
  }

  public void clear() {
    try (final Jedis jedis = jedisPool.getResource()) {
      jedis.flushAll();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() throws Exception {
    jedisPool.close();
  }
}
