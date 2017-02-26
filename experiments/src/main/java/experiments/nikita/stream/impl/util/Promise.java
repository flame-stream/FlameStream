package experiments.nikita.stream.impl.util;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Created by marnikitta on 04.11.16.
 */
public class Promise<S> {
  private S value;

  public Promise() {
  }

  public S get() {
    return value;
  }

  public void set(final S value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
            .append("value", value)
            .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    Promise<?> promise = (Promise<?>) o;

    return new EqualsBuilder()
            .append(value, promise.value)
            .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
            .append(value)
            .toHashCode();
  }
}
