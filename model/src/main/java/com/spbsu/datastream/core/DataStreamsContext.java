package com.spbsu.datastream.core;

import com.spbsu.commons.func.types.ConversionRepository;
import com.spbsu.commons.func.types.SerializationRepository;
import com.spbsu.commons.func.types.impl.TypeConvertersCollection;
import com.spbsu.commons.seq.CharSeq;
import com.spbsu.datastream.core.inference.DataTypeCollection;
import com.spbsu.datastream.core.inference.TypeCollection;
import com.spbsu.datastream.core.inference.sql.SqlInference;
import com.spbsu.datastream.core.io.Input;
import com.spbsu.datastream.core.io.UserLogInput;
import com.spbsu.datastream.core.io.Output;
import com.spbsu.datastream.core.io.OutputProcessor;
import com.spbsu.datastream.core.state.StateRepository;
import com.spbsu.datastream.example.bl.counter.UserQuery;
import com.spbsu.datastream.example.bl.inverted_index.WordContainer;

/**
 * Created by marnikitta on 14.11.16.
 */
public class DataStreamsContext {
  public static final SerializationRepository<CharSeq> serializatonRepository = new SerializationRepository<>(
          new TypeConvertersCollection(ConversionRepository.ROOT,
                  UserQuery.class.getPackage().getName() + ".io",
                  WordContainer.class.getPackage().getName() + ".io"),
          CharSeq.class
  );

  public static final TypeCollection typeCollection = new DataTypeCollection();

  public static final SqlInference sqlInference = new SqlInference();

  public static final Input input = new UserLogInput();

  public static final Output output = new OutputProcessor();

  public static final StateRepository stateRepository = new StateRepository();
}
