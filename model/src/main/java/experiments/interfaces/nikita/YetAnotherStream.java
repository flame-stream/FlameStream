package experiments.interfaces.nikita;


import experiments.interfaces.nikita.annotation.Midway;
import experiments.interfaces.nikita.annotation.Terminal;

import java.beans.Transient;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.*;
import java.util.stream.*;

/**
 * Created by marnikitta on 12.10.16.
 */
public interface YetAnotherStream<S> /*extends Stream<S>*/ {
    Type<S> type();

    @Terminal
    double correlationWithMeta(Function<? super S, ? extends Comparable> comparable, CorrelationType type);

    @Midway
    <T> YetAnotherStream<T> map(Filter<S, T> filter);

    @Terminal
    boolean isValid();
}
