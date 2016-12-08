package com.spbsu.datastream.core.type;

import com.spbsu.datastream.core.DataType;

/**
 * Created by marnikitta on 12/8/16.
 */
public interface ParametrizedDataType {
  DataType construct(Object... args);
}
