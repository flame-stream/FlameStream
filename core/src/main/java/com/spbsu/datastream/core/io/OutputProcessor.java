package com.spbsu.datastream.core.io;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.Joba;
import com.spbsu.datastream.core.job.control.ConditionTriggered;
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
public class OutputProcessor implements Output {
  private List<Runnable> barriers = new ArrayList<>();
  private Map<String, TLongObjectHashMap<List<List<DataItem>>>> knownStates = new HashMap<>();

  public void registerCommitHandler(Runnable r) {
    barriers.add(r);
  }

  public Consumer<? super Object> processor() {
    return (Consumer<Object>) this::processItem;
  }

  public void commit() {
    barriers.forEach(Runnable::run);
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

  public TLongObjectHashMap<List<List<DataItem>>> load(DataType type) {
    return knownStates.getOrDefault(type.name(), new TLongObjectHashMap<>());
  }

  volatile int jobaid = 0;

  public int registerJoba(Joba joba) {
    return jobaid++;
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
}
