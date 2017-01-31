package com.spbsu.datastream.core.io;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.Joba;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.List;
import java.util.function.Consumer;

/**
 * Created by Artem on 12.01.2017.
 */
public interface Output {
  void commit();

  Consumer<? super Object> processor();

  void registerCommitHandler(Runnable r);

  void save(DataType type, TLongObjectHashMap<List<List<DataItem>>> state);

  TLongObjectHashMap<List<List<DataItem>>> load(DataType type);

  int registerJoba(Joba joba);
}
