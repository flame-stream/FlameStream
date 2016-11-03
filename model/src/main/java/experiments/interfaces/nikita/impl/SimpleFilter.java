package experiments.interfaces.nikita.impl;

import experiments.interfaces.nikita.Filter;
import experiments.interfaces.nikita.Type;

/**
 * Created by marnikitta on 02.11.16.
 */
@FunctionalInterface
public interface SimpleFilter<T, R> extends Filter<T, R> {

    static <T> SimpleFilter<T, T> identity() {
        return t -> t;
    }

    @Override
    default boolean isConsumed(final Type<? extends T> type) {
        return true;
    }

    @Override
    default boolean isProduced(final Type<? extends R> type) {
        return true;
    }

    @Override
    default Type<R> a2b(final Type<? extends T> from) {
        return new EmptyType<>();
    }

    @Override
    default Type<T> b2a(final Type<? extends R> to) {
        return new EmptyType<>();
    }

    @Override
    R apply(final T t);
}
