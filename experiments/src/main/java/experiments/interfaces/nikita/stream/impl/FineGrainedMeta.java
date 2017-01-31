package experiments.interfaces.nikita.stream.impl;

import experiments.interfaces.nikita.stream.Meta;

import java.util.Comparator;

/**
 * Created by marnikitta on 02.11.16.
 */
public class FineGrainedMeta implements Meta<FineGrainedMeta> {
  private final long major;

  private final long minor;

  private FineGrainedMeta(final long major, final long minor) {
    this.minor = minor;
    this.major = major;
  }

  public FineGrainedMeta(final long major) {
    this(major, 0);
  }

  @Override
  public FineGrainedMeta incremented() {
    return new FineGrainedMeta(this.major, this.minor + 1);
  }

  public long major() {
    return major;
  }

  public long minor() {
    return minor;
  }

  @Override
  public int compareTo(final FineGrainedMeta o) {
    return Comparator.comparingLong(FineGrainedMeta::major)
            .thenComparingLong(FineGrainedMeta::minor)
            .compare(this, o);
  }
}
