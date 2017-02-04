package com.spbsu.datastream.core.io;

import com.spbsu.datastream.core.DataItem;
import com.spbsu.datastream.core.DataType;

import java.util.stream.Stream;

/**
 * Created by Artem on 12.01.2017.
 */
public interface Input {
  Stream<Stream<DataItem>> stream(DataType type);
}
