package com.spbsu.flamestream.runtime.state;

import akka.serialization.Serialization;
import com.spbsu.flamestream.core.data.meta.GlobalTime;
import com.spbsu.flamestream.runtime.config.HashUnit;
import com.spbsu.flamestream.runtime.graph.state.GroupingState;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import scala.util.Try;

import java.util.ArrayList;
import java.util.List;
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

    //Flink's default parameters
    final DBOptions dbOptions = new DBOptions().setUseFsync(false).setCreateIfMissing(true);
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(1);
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor("default".getBytes(), new ColumnFamilyOptions()));
    final List<ColumnFamilyHandle> stateColumnFamilyHandles = new ArrayList<>();

    try {
      rocksDB = RocksDB.open(dbOptions, pathToDb, columnFamilyDescriptors, stateColumnFamilyHandles);
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
