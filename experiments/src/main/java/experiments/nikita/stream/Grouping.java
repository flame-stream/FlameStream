package experiments.nikita.stream;

import java.util.function.ToIntFunction;

/**
 * Created by marnikitta on 26.10.16.
 */
public interface Grouping<S> extends ToIntFunction<S> {
  boolean equals(S s1, S s2);
}
