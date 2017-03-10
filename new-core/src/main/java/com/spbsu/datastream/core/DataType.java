package com.spbsu.datastream.core;

import java.util.Set;

/**
 * Experts League
 * Created by solar on 05.11.16.
 */
public interface DataType {
  String name();

  Set<Condition> conditions();
}
