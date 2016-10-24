package experiments.interfaces.nikita;


import experiments.interfaces.nikita.annotation.Midway;
import experiments.interfaces.nikita.annotation.Terminal;

import java.util.function.Function;

/**
 * Created by marnikitta on 12.10.16.
 */
public interface YetAnotherStream<S> /*extends Stream<S>*/ {
    Type<S> type();

    @Terminal
    double correlationWithMeta(Function<? super S, ? extends Comparable> comparable, CorrelationType type);

    @Midway
    <T> YetAnotherStream<T> map(Filter<S, T> filter);

//    YetAnotherStream<Iterator<S>> groupBy(Function<S, Integer> hash);

    @Terminal
    boolean isValid();
}
