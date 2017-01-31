package experiments.interfaces.nikita.stream.impl.util;

import experiments.interfaces.nikita.stream.impl.FineGrainedMeta;

import java.util.function.Supplier;

/**
 * Created by marnikitta on 04.11.16.
 */
public class MetaGenerator implements Supplier<FineGrainedMeta> {
  private long major = 0;

  @Override
  public FineGrainedMeta get() {
    return new FineGrainedMeta(major++);
  }
}
