package experiments.interfaces.nikita;

import java.util.function.Function;

/**
 * Created by marnikitta on 26.10.16.
 */
public interface Grouping<S> extends Function<S, Integer> {
    boolean equals(S s1, S s2);
}
