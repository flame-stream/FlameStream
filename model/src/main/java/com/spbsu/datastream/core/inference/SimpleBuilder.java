package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataStreamsContext;
import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.exceptions.TypeUnreachableException;
import com.spbsu.datastream.core.job.IdentityJoba;
import com.spbsu.datastream.core.job.Joba;
import org.jooq.lambda.Seq;

import javax.inject.Inject;
import java.util.List;

/**
 * Created by marnikitta on 28.11.16.
 */
public class SimpleBuilder implements JobaBuilder {
  @Inject
  private final TypeCollection collection = DataStreamsContext.typeCollection;

  private final TypeGraph graph = new TypeGraph();

  @Override
  public Joba build(final DataType from, final DataType to) throws TypeUnreachableException {
    final List<Morphism> path = graph.findPath(from, to);
    if (path.size() == 0) {
      throw new TypeUnreachableException();
    } else {
      return Seq.seq(path.stream()).foldLeft((Joba) new IdentityJoba(from), (j, m) -> m.apply(j));
    }
  }

  public void index() {
    collection.loadMorphisms().forEach(graph::addMorphism);
  }
}
