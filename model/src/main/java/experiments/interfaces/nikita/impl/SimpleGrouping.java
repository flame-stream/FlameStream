package experiments.interfaces.nikita.impl;

import experiments.interfaces.nikita.Grouping;

/**
 * Created by marnikitta on 03.11.16.
 */
@FunctionalInterface
public interface SimpleGrouping<S> extends Grouping<S>{

    @Override
    default boolean equals(S s1, S s2) {
        return s1.equals(s2);
    }
}
