package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.exceptions.TypeUnreachableException;
import com.spbsu.datastream.core.job.Joba;

/**
 * Created by marnikitta on 28.11.16.
 */
public interface JobaBuilder {
  Joba build(DataType from, DataType to) throws TypeUnreachableException;
}
