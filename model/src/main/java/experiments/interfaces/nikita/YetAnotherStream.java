package experiments.interfaces.nikita;


import experiments.interfaces.nikita.annotation.Midway;
import experiments.interfaces.nikita.annotation.Terminal;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.BaseStream;
import java.util.stream.Collector;

/**
 * Created by marnikitta on 12.10.16.
 */
public interface YetAnotherStream<S> extends BaseStream<DataItem<S>, YetAnotherStream<S>> {
    Type<S> type();

    @Midway
    <T> YetAnotherStream<T> filter(Filter<S, T> filter);

    @Midway
    YetAnotherStream<S> split();

    @Terminal
    void forEach(Consumer<? super S> consumer);

    @Terminal
    <R, A> R collect(Collector<? super S, A, R> collector);

    @Midway
    YetAnotherStream<S> peek(Consumer<? super S> action);


    YetAnotherStream<S> mergeWith(YetAnotherStream<S> that);

    @Midway
    YetAnotherStream<List<S>> groupBy(Grouping<S> grouping, int window);

    @Terminal
    boolean isValid();
}
