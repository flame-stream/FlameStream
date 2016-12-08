package com.spbsu.datastream.core.io;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.Joba;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class Output {
  private static Output instance = new Output();
  private List<Runnable> barriers = new ArrayList<>();
  private Map<String, TLongObjectHashMap<List<List<DataItem>>>> knownStates = new HashMap<>();
  private boolean done = false;

  public void registerCommitHandler(Runnable r) {
    barriers.add(r);
  }

  public Consumer<? super DataItem> printer() {
    return (Consumer<DataItem>) dataItem -> System.out.println(dataItem.serializedData());
  }

  public void commit() {
    barriers.forEach(Runnable::run);
    if (done) {
      System.exit(0);
    }
  }

  public static synchronized Output instance() {
    return instance;
  }

  public void save(DataType type, TLongObjectHashMap<List<List<DataItem>>> state) {
    knownStates.put(type.name(), state);
    System.out.println("Saving state for [" + type.name() + "]:");
    state.forEachEntry((i, dataItems) -> {
      dataItems.forEach(new Consumer<List<DataItem>>() {
        int slot = 0;
        @Override
        public void accept(List<DataItem> dataItem) {
          System.out.println("\t" + i + ":" + slot + " " + dataItem);
          slot++;
        }
      });
      return true;
    });
  }

  public void done() {
    done = true;
  }

  public TLongObjectHashMap<List<List<DataItem>>> load(DataType type) {
    return knownStates.getOrDefault(type.name(), new TLongObjectHashMap<>());
  }

  volatile int jobaid = 0;
  public int registerJoba(Joba joba) {
    return jobaid++;
  }
}
