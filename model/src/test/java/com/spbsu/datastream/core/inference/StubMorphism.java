package com.spbsu.datastream.core.inference;

import com.spbsu.datastream.core.DataType;
import com.spbsu.datastream.core.job.Joba;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by marnikitta on 28.11.16.
 */
public class StubMorphism implements Morphism {
  private final DataType from;

  private final DataType to;

  public StubMorphism(final DataType from, final DataType to) {
    this.from = from;
    this.to = to;
  }

  @Override
  public DataType consumes() {
    return from;
  }

  @Override
  public DataType supplies() {
    return to;
  }

  @Override
  public Joba apply(final Joba joba) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    StubMorphism that = (StubMorphism) o;

    return new EqualsBuilder()
            .append(from, that.from)
            .append(to, that.to)
            .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
            .append(from)
            .append(to)
            .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
            .append("from", from)
            .append("to", to)
            .toString();
  }
}
