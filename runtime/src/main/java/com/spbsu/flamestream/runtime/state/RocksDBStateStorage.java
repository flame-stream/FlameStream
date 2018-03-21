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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBStateStorage implements StateStorage {
  private final RocksDB rocksDB;
  private final Serialization serialization;
  private final Map<HashUnit, GlobalTime> prevTimeForUnit = new HashMap<>();

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
    final String key = key(unit, time);
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
    final String key = key(unit, time);
    final byte[] data = serialization.serialize(state).get();
    try {
      rocksDB.put(key.getBytes(), data);

      final GlobalTime prevTime = prevTimeForUnit.get(unit);
      if (prevTime != null) {
        rocksDB.deleteRange(unit.toString().getBytes(), key(unit, prevTime).getBytes());
      }
      prevTimeForUnit.put(unit, time);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  private static String key(HashUnit hashUnit, GlobalTime globalTime) {
    return hashUnit.id() + '@' + globalTime.toString();
  }

  @Override
  public void close() {
    rocksDB.close();
  }
}
