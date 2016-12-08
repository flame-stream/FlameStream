package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.exceptions.TypeUnreachableException;
import com.spbsu.datastream.core.job.Joba;

import javax.inject.Inject;

/**
 * Created by marnikitta on 28.11.16.
 */
public class SimpleBuilder implements JobaBuilder {
  @Inject
  private final TypeCollection collection = DataStreamsContext.typeCollection;

  @Override
  public Joba build(final DataType from, final DataType to) throws TypeUnreachableException {
    throw new UnsupportedOperationException();
  }
}

