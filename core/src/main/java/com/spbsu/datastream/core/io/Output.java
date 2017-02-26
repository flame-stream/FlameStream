package com.spbsu.datastream.core.io;

import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.Joba;
import com.spbsu.datastream.core.job.grouping_storage.GroupingStorage;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Created by Artem on 12.01.2017.
 */
public interface Output {
  void commit();

  Consumer<? super Object> processor();

  void registerCommitHandler(Runnable r);

  void save(DataType type, GroupingStorage state);

  Optional<GroupingStorage> load(DataType type);

  int registerJoba(Joba joba);
}
