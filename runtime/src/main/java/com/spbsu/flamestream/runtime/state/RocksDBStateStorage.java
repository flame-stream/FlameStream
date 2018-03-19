package com.spbsu.flamestream.runtime.state;

import akka.serialization.Serialization;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.graph.state.GroupingState;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import scala.util.Try;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBStateStorage implements StateStorage {
  private final RocksDB rocksDB;
  private final Serialization serialization;

  static {
    RocksDB.loadLibrary();
  }

  public RocksDBStateStorage(String pathToDb, Serialization serialization) {
    this.serialization = serialization;
    final Options options = new Options().setCreateIfMissing(true);
    try {
      rocksDB = RocksDB.open(options, pathToDb);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<String, GroupingState> stateFor(HashUnit unit, GlobalTime time) {
    final String key = unit.id() + '@' + time.toString();
    try {
      final byte[] data = rocksDB.get(key.getBytes());
      if (data != null) {
        final Try<Map> trie = serialization.deserialize(data, Map.class);
        //noinspection unchecked
        return (Map<String, GroupingState>) trie.get();
      } else {
        return new ConcurrentHashMap<>();
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void putState(HashUnit unit, GlobalTime time, Map<String, GroupingState> state) {
    final String key = unit.id() + '@' + time.toString();
    final byte[] data = serialization.serialize(state).get();
    try {
      rocksDB.put(key.getBytes(), data);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    rocksDB.close();
  }
}
