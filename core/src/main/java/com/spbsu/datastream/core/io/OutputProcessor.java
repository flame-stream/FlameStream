package com.spbsu.datastream.core.io;

import com.spbsu.commons.io.StreamTools;
import com.spbsu.commons.util.cache.CacheStrategy;
import com.spbsu.commons.util.cache.impl.FixedSizeCache;
import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.Joba;
import com.spbsu.datastream.core.job.control.ConditionTriggered;
import com.spbsu.datastream.core.job.grouping_storage.GroupingStorage;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.DbImpl;

import java.io.*;
import java.util.*;
import java.util.function.Consumer;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class OutputProcessor implements Output, Closeable {
  private final List<Runnable> barriers = new ArrayList<>();
  private final FixedSizeCache<String, byte[]> keysCache = new FixedSizeCache<>(10000, CacheStrategy.Type.LRU);
  private final DB db;

  public OutputProcessor() {
    try {
      db = new DbImpl(new Options().createIfMissing(true), new File("./leveldb"));
    } catch (IOException e) {
      throw new RuntimeException("LevelDB is not initialized: " + e);
    }
  }

  public void registerCommitHandler(Runnable r) {
    barriers.add(r);
  }

  public Consumer<? super Object> processor() {
    return (Consumer<Object>) this::processItem;
  }

  public void commit() {
    barriers.forEach(Runnable::run);
  }

  public void saveState(DataType type, GroupingStorage state) {
    System.out.println("Saving state for [" + type.name() + "]:");
    state.forEach(System.out::println);

    final byte[] key = keysCache.get(type.name(), argument -> argument.getBytes(StreamTools.UTF));
    final ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      final ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(state);
      oos.close();

      final byte[] value = bos.toByteArray();
      bos.close();
      db.put(key, value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Optional<GroupingStorage> loadState(DataType type) {
    final byte[] key = keysCache.get(type.name(), argument -> argument.getBytes(StreamTools.UTF));
    final byte[] value = db.get(key);
    if (value != null) {
      final ByteArrayInputStream in = new ByteArrayInputStream(value);
      try {
        final ObjectInputStream is = new ObjectInputStream(in);
        final GroupingStorage storage = (GroupingStorage) is.readObject();
        is.close();
        in.close();
        return Optional.of(storage);
      } catch (IOException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    } else {
      return Optional.empty();
    }
  }

  public void removeState(DataType type) {
    final byte[] key = keysCache.get(type.name(), argument -> argument.getBytes(StreamTools.UTF));
    db.delete(key);
  }

  private volatile int jobaId = 0;

  public int registerJoba(Joba joba) {
    return jobaId++;
  }

  private void processItem(Object item) {
    if (item instanceof DataItem) {
      System.out.println(((DataItem) item).serializedData());
    } else if (item instanceof ConditionTriggered) {
      if (((ConditionTriggered) item).condition().success()) {
        commit();
      }
      System.exit(0);
    }
  }

  @Override
  public void close() throws IOException {
    db.close();
  }
}
