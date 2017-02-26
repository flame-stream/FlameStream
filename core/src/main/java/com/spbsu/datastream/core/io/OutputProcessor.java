package com.spbsu.datastream.core.io;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.Joba;
import com.spbsu.datastream.core.job.control.ConditionTriggered;
import com.spbsu.datastream.core.job.grouping_storage.GroupingStorage;
import gnu.trove.map.hash.THashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public class OutputProcessor implements Output {
  private List<Runnable> barriers = new ArrayList<>();
  private Map<String, GroupingStorage> knownStates = new THashMap<>();

  public void registerCommitHandler(Runnable r) {
    barriers.add(r);
  }

  public Consumer<? super Object> processor() {
    return (Consumer<Object>) this::processItem;
  }

  public void commit() {
    barriers.forEach(Runnable::run);
  }

  public void save(DataType type, GroupingStorage state) {
    knownStates.put(type.name(), state);
    System.out.println("Saving state for [" + type.name() + "]:");
    state.forEach(System.out::println);
  }

  public Optional<GroupingStorage> load(DataType type) {
    final GroupingStorage storage = knownStates.get(type.name());
    if (storage != null) {
      return Optional.of(storage);
    } else {
      return Optional.empty();
    }
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
}
