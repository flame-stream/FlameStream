package com.spbsu.datastream.core;

import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.datastream.core.inference.TypeCollection;
import com.spbsu.datastream.core.io.Input;
import com.spbsu.datastream.core.io.Output;
import com.spbsu.datastream.core.io.OutputProcessor;
import com.spbsu.datastream.core.io.UserLogInput;

/**
 * Created by marnikitta on 14.11.16.
 */
public class DataStreamsContext {
  public static SerializationRepository<CharSeq> serializatonRepository;

  public static final TypeCollection typeCollection = new DataTypeCollection();

  public static final Input input = new UserLogInput();

  public static final Output output = new OutputProcessor();
}
